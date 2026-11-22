/**
 * LocalShred Lite & ShredStream live feed — zero dependencies (node:http2 + node:dgram + minimal protobuf/bincode codec).
 *
 * Supports both LocalShred Lite deployments automatically:
 *   1. Direct Heartbeat + UDP Data Shred Deshredder (`shredstream.Shredstream/SendHeartbeat`):
 *      Sends HTTP/2 gRPC heartbeats with header `x-localshred-auth: <apiKey>` to the upstream
 *      LocalShred Lite server (e.g. `http://localshred-lite.nlams.dedicatednodes.io:9999`),
 *      receives raw UDP Solana data shreds on `udpPort` (default 20000), reassembles contiguous
 *      data shred batches up to `DATA_COMPLETE_SHRED` (flag 0x40), and decodes bincode `Vec<Entry>`.
 *   2. Local Proxy gRPC Entry Stream (`shredstream.ShredstreamProxy/SubscribeEntries`):
 *      When `proxyUrl` is provided (or when `mode === 'entries'`), streams pre-deshredded entries
 *      from a local `shredstream-proxy` (`--grpc-service-port`) and decodes bincode `Vec<Entry>`.
 *
 * Pre-Execution Fast Swap Decoding:
 *   ShredStream delivers raw `VersionedTransaction` wire bytes BEFORE leader execution (no `meta`,
 *   no logs, no pre/post balances yet). When a watched trader signs a Pump.fun bonding-curve buy/sell
 *   (either directly via `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` or via `FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9`),
 *   `extractSwapFromRawTx` decodes the exact mint, side, SOL lamports, token amount, and token program
 *   directly from the raw instruction bytes so the watcher can trigger an immediate 0-RPC pre-execution
 *   copy (`shredTrade`) or race `processSignature` as the 4th live feed (`feedRace.shred`).
 */
import http2 from 'node:http2';
import dgram from 'node:dgram';
import { base58Encode } from './onchainMeta.mjs';
import { pbFields } from './yellowstone.mjs';

export const DEFAULT_LOCALSHRED_URL = 'http://localshred-lite.nlams.dedicatednodes.io:9999';
export const PUMP_PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
export const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
export const FLASHX_PROGRAM = 'FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9';
export const WSOL_MINT = 'So11111111111111111111111111111111111111112';
export const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
export const TOKEN_2022_PROGRAM = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const LAMPORTS_PER_SOL = 1_000_000_000;

// Anchor 8-byte instruction discriminators for direct Pump.fun bonding curve instructions
const DISC_BUY_EXACT_QUOTE_IN_V3 = Buffer.from([225, 247, 80, 30, 213, 179, 132, 136]);
const DISC_BUY_V1 = Buffer.from([102, 6, 61, 18, 1, 218, 235, 234]);
const DISC_SELL_V1_V3 = Buffer.from([51, 230, 133, 164, 1, 127, 131, 173]);
const DISC_SELL_V3_ALT = Buffer.from([28, 146, 222, 119, 38, 196, 105, 213]);

// ---------- minimal protobuf helpers ----------
function varint(n) {
  let v = BigInt(n);
  const out = [];
  do {
    let b = Number(v & 0x7fn);
    v >>= 7n;
    if (v) b |= 0x80;
    out.push(b);
  } while (v);
  return Buffer.from(out);
}
const tag = (field, wire) => varint((field << 3) | wire);
const fVarint = (field, n) => Buffer.concat([tag(field, 0), varint(n)]);
const fBytes = (field, buf) => Buffer.concat([tag(field, 2), varint(buf.length), buf]);
const fString = (field, s) => fBytes(field, Buffer.from(String(s), 'utf8'));
const first = (fields, n) => fields.find((f) => f.field === n);

export const grpcFrame = (msg) => {
  const h = Buffer.alloc(5);
  h.writeUInt32BE(msg.length, 1);
  return Buffer.concat([h, msg]);
};

/**
 * Encode `shredstream.Heartbeat`:
 *   message Socket { string ip = 1; int64 port = 2; }
 *   message Heartbeat { shared.Socket socket = 1; repeated string regions = 2; }
 */
export function encodeHeartbeat({ ip = '0.0.0.0', port = 20000, regions = [] } = {}) {
  const socket = Buffer.concat([fString(1, ip), fVarint(2, port)]);
  const parts = [fBytes(1, socket)];
  for (const r of regions) {
    if (r) parts.push(fString(2, r));
  }
  return Buffer.concat(parts);
}

/** Decode `shredstream.HeartbeatResponse { uint32 ttl_ms = 1; }` */
export function decodeHeartbeatResponse(buf) {
  const f = pbFields(buf);
  const ttl = first(f, 1);
  return { ttlMs: ttl ? Number(ttl.value) : 1000 };
}

/** Decode `shredstream.Entry { uint64 slot = 1; bytes entries = 2; }` */
export function decodeEntryMessage(buf) {
  const f = pbFields(buf);
  const slotField = first(f, 1);
  const entriesField = first(f, 2);
  return {
    slot: slotField ? Number(slotField.value) : null,
    entriesBuf: entriesField && entriesField.wire === 2 ? entriesField.value : Buffer.alloc(0),
  };
}

// ---------- Solana Compact-u16 & Bincode Vec<Entry> / VersionedTransaction Decoder ----------

function readCompactU16(buf, offset) {
  let val = 0;
  let shift = 0;
  let o = offset;
  for (let i = 0; i < 3; i++) {
    if (o >= buf.length) throw new Error('truncated compact-u16');
    const b = buf[o++];
    val |= (b & 0x7f) << shift;
    if ((b & 0x80) === 0) return [val, o];
    shift += 7;
  }
  throw new Error('invalid compact-u16');
}

/**
 * Parse one serialized Solana `VersionedTransaction` starting at `offset` in `buf`.
 * Returns `{ tx, nextOffset }` where `tx` has:
 *   `signature` (base58), `version` ('legacy' | 0 | 1), `numRequiredSignatures`,
 *   `staticAccountKeys` (base58[]), `signers` (base58[]), `recentBlockhash` (base58),
 *   `instructions` ([{ programIdIndex, programId, accountIndices, data }]),
 *   `addressTableLookups` ([{ accountKey, writableIndexes, readonlyIndexes }]).
 */
export function decodeVersionedTransactionAt(buf, offset = 0) {
  let o = offset;
  const [numSigs, o1] = readCompactU16(buf, o);
  o = o1;
  if (numSigs < 1 || numSigs > 64 || o + numSigs * 64 > buf.length) {
    throw new Error('invalid signature count');
  }
  const firstSigBytes = buf.subarray(o, o + 64);
  o += numSigs * 64;

  if (o >= buf.length) throw new Error('truncated message header');
  const firstByte = buf[o];
  let version = 'legacy';
  if (firstByte & 0x80) {
    version = firstByte & 0x7f;
    if (version > 1) throw new Error(`unsupported transaction version ${version}`);
    o += 1;
  }
  if (o + 3 > buf.length) throw new Error('truncated message header');
  const numRequiredSignatures = buf[o];
  const numReadonlySignedAccounts = buf[o + 1];
  const numReadonlyUnsignedAccounts = buf[o + 2];
  o += 3;

  const [numKeys, o2] = readCompactU16(buf, o);
  o = o2;
  if (numKeys < numRequiredSignatures || numKeys > 256 || o + numKeys * 32 > buf.length) {
    throw new Error('invalid account key count');
  }
  const rawKeys = [];
  for (let i = 0; i < numKeys; i++) {
    rawKeys.push(buf.subarray(o, o + 32));
    o += 32;
  }

  if (o + 32 > buf.length) throw new Error('truncated blockhash');
  const rawBlockhash = buf.subarray(o, o + 32);
  o += 32;

  const [numIx, o3] = readCompactU16(buf, o);
  o = o3;
  if (numIx > 128) throw new Error('invalid instruction count');
  const rawInstructions = [];
  for (let i = 0; i < numIx; i++) {
    if (o >= buf.length) throw new Error('truncated instruction programIdIndex');
    const programIdIndex = buf[o++];
    const [numAcc, oAcc] = readCompactU16(buf, o);
    o = oAcc;
    if (o + numAcc > buf.length) throw new Error('truncated instruction accounts');
    const accountIndices = Array.from(buf.subarray(o, o + numAcc));
    o += numAcc;
    const [dataLen, oData] = readCompactU16(buf, o);
    o = oData;
    if (o + dataLen > buf.length) throw new Error('truncated instruction data');
    const data = buf.subarray(o, o + dataLen);
    o += dataLen;
    rawInstructions.push({ programIdIndex, accountIndices, data });
  }

  const addressTableLookups = [];
  if (version === 0) {
    const [numLookups, o4] = readCompactU16(buf, o);
    o = o4;
    if (numLookups > 64) throw new Error('invalid lookup count');
    for (let i = 0; i < numLookups; i++) {
      if (o + 32 > buf.length) throw new Error('truncated lookup key');
      const accountKey = base58Encode(buf.subarray(o, o + 32));
      o += 32;
      const [wLen, oW] = readCompactU16(buf, o);
      o = oW;
      if (o + wLen > buf.length) throw new Error('truncated writable lookup');
      const writableIndexes = Array.from(buf.subarray(o, o + wLen));
      o += wLen;
      const [rLen, oR] = readCompactU16(buf, o);
      o = oR;
      if (o + rLen > buf.length) throw new Error('truncated readonly lookup');
      const readonlyIndexes = Array.from(buf.subarray(o, o + rLen));
      o += rLen;
      addressTableLookups.push({ accountKey, writableIndexes, readonlyIndexes });
    }
  } else if (version === 1) {
    // SIMD-0134 v1 message config mask (u32LE) if present
    if (o + 4 <= buf.length) o += 4;
  }

  const staticAccountKeys = rawKeys.map((k) => base58Encode(k));
  const signers = staticAccountKeys.slice(0, numRequiredSignatures);
  const instructions = rawInstructions.map((ix) => ({
    programIdIndex: ix.programIdIndex,
    programId: staticAccountKeys[ix.programIdIndex] || null,
    accountIndices: ix.accountIndices,
    data: ix.data,
  }));

  return {
    tx: {
      signature: base58Encode(firstSigBytes),
      version,
      numRequiredSignatures,
      numReadonlySignedAccounts,
      numReadonlyUnsignedAccounts,
      staticAccountKeys,
      signers,
      recentBlockhash: base58Encode(rawBlockhash),
      instructions,
      addressTableLookups,
    },
    nextOffset: o,
  };
}

/**
 * Decode bincode `Vec<solana_entry::entry::Entry>` into an array of decoded `VersionedTransaction`s.
 * Layout:
 *   u64LE entryCount
 *   repeated Entry:
 *     u64LE num_hashes (8 bytes)
 *     [u8; 32] hash (32 bytes)
 *     u64LE txCount (8 bytes)
 *     repeated VersionedTransaction (txCount items)
 */
export function decodeBincodeEntries(buf) {
  if (!Buffer.isBuffer(buf) || buf.length < 8) return [];
  let o = 0;
  const entryCount = Number(buf.readBigUInt64LE(o));
  o += 8;
  if (entryCount < 0 || entryCount > 32_768) return [];
  const txs = [];
  for (let e = 0; e < entryCount; e++) {
    if (o + 48 > buf.length) break;
    // skip num_hashes (8) + hash (32)
    o += 40;
    const txCount = Number(buf.readBigUInt64LE(o));
    o += 8;
    if (txCount < 0 || txCount > 16_384) break;
    for (let t = 0; t < txCount; t++) {
      const { tx, nextOffset } = decodeVersionedTransactionAt(buf, o);
      txs.push(tx);
      o = nextOffset;
    }
  }
  return txs;
}

// ---------- Raw UDP Data Shred Parser & Per-Slot Reassembler ----------

/**
 * Parse a single raw Solana UDP shred packet (`<= 1228` bytes).
 * Returns `{ slot, index, version, fecSetIndex, parentOffset, flags, dataComplete, lastInSlot, payload }`
 * for Data Shreds (legacy `0xa5` or Merkle `0x80..0xbf`), or `null` for Coding Shreds / invalid packets.
 */
export function parseDataShred(pkt) {
  if (!Buffer.isBuffer(pkt) || pkt.length < 88) return null;
  const variant = pkt[64];
  const isLegacyData = variant === 0xa5;
  const isMerkleData = (variant & 0xc0) === 0x80; // high two bits 10 = Merkle Data Shred
  if (!isLegacyData && !isMerkleData) return null;

  const slot = Number(pkt.readBigUInt64LE(65));
  const index = pkt.readUInt32LE(73);
  const version = pkt.readUInt16LE(77);
  const fecSetIndex = pkt.readUInt32LE(79);
  const parentOffset = pkt.readUInt16LE(83);
  const flags = pkt[85];
  const totalSize = pkt.readUInt16LE(86);

  if (totalSize < 88 || totalSize > pkt.length) return null;
  const payload = pkt.subarray(88, totalSize);
  const dataComplete = (flags & 0x40) !== 0;
  const lastInSlot = (flags & 0xc0) === 0xc0;

  return {
    slot,
    index,
    version,
    fecSetIndex,
    parentOffset,
    flags,
    dataComplete,
    lastInSlot,
    payload,
  };
}

/**
 * Reassembles contiguous data shreds per slot into complete `Vec<Entry>` bincode buffers.
 * In Solana's turbine/shredstream, each `Vec<Entry>` batch starts at index 0 (or immediately after
 * the previous `DATA_COMPLETE_SHRED` index) and ends at a shred with `flags & 0x40` (`DATA_COMPLETE_SHRED`).
 */
export class DataShredAssembler {
  constructor({ maxSlots = 48 } = {}) {
    this.maxSlots = maxSlots;
    this.slots = new Map(); // slot -> { shreds: Map<index, { payload, dataComplete }>, consumedRanges: Set<string>, nextStart: number, updatedAt: number }
  }

  /**
   * Push a raw UDP packet. If it completes one or more contiguous data shred batches,
   * returns `[{ slot, entriesBuf, txs }]`.
   */
  pushPacket(pkt) {
    const shred = parseDataShred(pkt);
    if (!shred) return [];
    let state = this.slots.get(shred.slot);
    if (!state) {
      state = { shreds: new Map(), completedEnds: new Set(), updatedAt: Date.now() };
      this.slots.set(shred.slot, state);
      if (this.slots.size > this.maxSlots) {
        const oldestSlot = this.slots.keys().next().value;
        this.slots.delete(oldestSlot);
      }
    }
    state.updatedAt = Date.now();
    if (state.shreds.has(shred.index)) return [];
    state.shreds.set(shred.index, shred);

    // Find all dataComplete indices in this slot that haven't been emitted yet
    const completeIndices = [];
    for (const [idx, s] of state.shreds) {
      if (s.dataComplete && !state.completedEnds.has(idx)) {
        completeIndices.push(idx);
      }
    }
    if (!completeIndices.length) return [];
    completeIndices.sort((a, b) => a - b);

    const out = [];
    for (const endIdx of completeIndices) {
      // Walk backward from endIdx to find the start of this batch (either index 0 or the index right after a previous dataComplete shred)
      let startIdx = endIdx;
      let contiguous = true;
      while (startIdx > 0) {
        const prev = state.shreds.get(startIdx - 1);
        if (!prev) {
          contiguous = false;
          break;
        }
        if (prev.dataComplete) {
          break; // startIdx is the first shred of the current batch!
        }
        startIdx -= 1;
      }
      if (!contiguous) continue;

      const chunks = [];
      for (let i = startIdx; i <= endIdx; i++) {
        chunks.push(state.shreds.get(i).payload);
      }
      state.completedEnds.add(endIdx);
      const entriesBuf = Buffer.concat(chunks);
      let txs = [];
      try {
        txs = decodeBincodeEntries(entriesBuf);
      } catch {
        txs = [];
      }
      if (txs.length) {
        out.push({ slot: shred.slot, entriesBuf, txs });
      }
    }
    return out;
  }
}

// ---------- Pre-Execution Raw Transaction Swap Extractor ----------

/**
 * Extract a pre-execution Pump.fun swap (`buy` or `sell`) directly from a raw decoded
 * `VersionedTransaction` signed by `wallet` (before execution / without `meta` or logs).
 *
 * Recognizes:
 *   1. Direct Pump.fun (`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`) top-level instructions:
 *      - `buy_exact_quote_in_v3` (disc `225,247,80,30,213,179,132,136`):
 *        accounts[1] = mint, accounts[2] = quoteMint, accounts[3] = tokenProgram,
 *        data[8..16] = lamportsIn (u64LE), data[16..24] = minTokensOut (u64LE)
 *      - `buy` v1/v2 (disc `102,6,61,18,1,218,235,234`):
 *        accounts[2] = mint, data[8..16] = tokenAmount (u64LE), data[16..24] = maxSolCost (u64LE)
 *      - `sell` / `sell_v3` (disc `51,230,133,164,1,127,131,173` or `28,146,222,119,38,196,105,213`):
 *        v3 (17 accounts): accounts[1] = mint, accounts[3] = tokenProgram; v1 (12+ accounts): accounts[2] = mint;
 *        data[8..16] = tokenAmount (u64LE), data[16..24] = minSolOutput (u64LE)
 *   2. FLASHX (`FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9`) router instructions (used by `@Decu`):
 *      - When `staticAccountKeys` includes `PUMP_PROGRAM` and `wallet` is a signer:
 *        - FLASHX ATA instruction (`data.length === 1 && data[0] === 5`, 7 accounts):
 *          accounts[3] = mint, accounts[5] = tokenProgram
 *        - FLASHX swap instruction (`data.length >= 18`, 20+ accounts where `accounts` includes `PUMP_PROGRAM`):
 *          `data[0] === 0` (buy): `lamportsIn = data.readBigUInt64LE(1)`, `minTokensOut = data.readBigUInt64LE(10)`
 *          Account #7 (`accounts[7]`) is `mint`, Account #8 (`accounts[8]`) is `quoteMint`, Account #9 (`accounts[9]`) is `tokenProgram`.
 *
 * Returns a `knownSwap`-compatible swap object `{ side, tokenMint, tokenDecimals, tokenAmount, quoteSymbol, quoteMint, quoteAmount, priceInQuote, venue, signature, slot, blockTime, curve, preExecution: true }`
 * or `null` when the transaction cannot be pre-parsed from instructions alone (in which case the watcher
 * still queues `signature` for fast RPC / gRPC confirmation).
 */
export function extractSwapFromRawTx(rawTx, wallet, { slot = null } = {}) {
  if (!rawTx || !Array.isArray(rawTx.signers) || !rawTx.signers.includes(wallet)) return null;
  const keys = rawTx.staticAccountKeys || [];
  const keyAt = (idx) => (Number.isInteger(idx) && idx >= 0 && idx < keys.length ? keys[idx] : null);
  const detectedTokenProgram = keys.includes(TOKEN_2022_PROGRAM)
    ? TOKEN_2022_PROGRAM
    : keys.includes(TOKEN_PROGRAM)
      ? TOKEN_PROGRAM
      : TOKEN_2022_PROGRAM;

  // 1. Check direct top-level Pump.fun instructions
  for (const ix of rawTx.instructions || []) {
    if (ix.programId !== PUMP_PROGRAM || !Buffer.isBuffer(ix.data) || ix.data.length < 24) continue;
    const disc = ix.data.subarray(0, 8);
    if (disc.equals(DISC_BUY_EXACT_QUOTE_IN_V3)) {
      const mint = keyAt(ix.accountIndices[1]);
      const quoteMint = keyAt(ix.accountIndices[2]) || WSOL_MINT;
      const tokenProgram = keyAt(ix.accountIndices[3]) || detectedTokenProgram;
      if (!mint || quoteMint !== WSOL_MINT) continue;
      const lamportsIn = ix.data.readBigUInt64LE(8);
      const minTokensOut = ix.data.readBigUInt64LE(16);
      if (lamportsIn <= 0n) continue;
      const solAmt = Number(lamportsIn) / LAMPORTS_PER_SOL;
      const tokAmt = minTokensOut > 0n ? Number(minTokensOut) / 1e6 : 1;
      return {
        side: 'buy',
        sellSharePct: null,
        tokenMint: mint,
        tokenDecimals: 6,
        tokenAmount: tokAmt,
        quoteSymbol: 'SOL',
        quoteMint: WSOL_MINT,
        quoteAmount: solAmt,
        poolQuoteAmount: solAmt,
        priceInQuote: tokAmt > 0 ? solAmt / tokAmt : 0,
        venue: 'pumpfun',
        signature: rawTx.signature,
        slot,
        blockTime: Date.now(),
        curve: { tokenProgram, at: Date.now() },
        preExecution: true,
      };
    }
    if (disc.equals(DISC_BUY_V1)) {
      const mint = keyAt(ix.accountIndices[2]);
      if (!mint) continue;
      const tokenRaw = ix.data.readBigUInt64LE(8);
      const maxSolCost = ix.data.readBigUInt64LE(16);
      if (tokenRaw <= 0n || maxSolCost <= 0n) continue;
      const solAmt = Number(maxSolCost) / LAMPORTS_PER_SOL;
      const tokAmt = Number(tokenRaw) / 1e6;
      return {
        side: 'buy',
        sellSharePct: null,
        tokenMint: mint,
        tokenDecimals: 6,
        tokenAmount: tokAmt,
        quoteSymbol: 'SOL',
        quoteMint: WSOL_MINT,
        quoteAmount: solAmt,
        poolQuoteAmount: solAmt,
        priceInQuote: tokAmt > 0 ? solAmt / tokAmt : 0,
        venue: 'pumpfun',
        signature: rawTx.signature,
        slot,
        blockTime: Date.now(),
        curve: { tokenProgram: detectedTokenProgram, at: Date.now() },
        preExecution: true,
      };
    }
    if (disc.equals(DISC_SELL_V1_V3) || disc.equals(DISC_SELL_V3_ALT)) {
      const isV3 = ix.accountIndices.length >= 16 && keyAt(ix.accountIndices[2]) === WSOL_MINT;
      const mint = keyAt(ix.accountIndices[isV3 ? 1 : 2]);
      const tokenProgram = (isV3 && keyAt(ix.accountIndices[3])) || detectedTokenProgram;
      if (!mint) continue;
      const tokenRaw = ix.data.readBigUInt64LE(8);
      const minSolOut = ix.data.readBigUInt64LE(16);
      if (tokenRaw <= 0n) continue;
      const tokAmt = Number(tokenRaw) / 1e6;
      const solAmt = minSolOut > 0n ? Number(minSolOut) / LAMPORTS_PER_SOL : 0.01;
      return {
        side: 'sell',
        sellSharePct: 100,
        tokenMint: mint,
        tokenDecimals: 6,
        tokenAmount: tokAmt,
        quoteSymbol: 'SOL',
        quoteMint: WSOL_MINT,
        quoteAmount: solAmt,
        poolQuoteAmount: solAmt,
        priceInQuote: tokAmt > 0 ? solAmt / tokAmt : 0,
        venue: 'pumpfun',
        signature: rawTx.signature,
        slot,
        blockTime: Date.now(),
        curve: { tokenProgram, at: Date.now() },
        preExecution: true,
      };
    }
  }

  // 2. Check FLASHX router instructions calling Pump.fun (used by @Decu: 4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9)
  if (keys.includes(FLASHX_PROGRAM) && keys.includes(PUMP_PROGRAM)) {
    let ataMint = null;
    let ataTokenProgram = null;
    for (const ix of rawTx.instructions || []) {
      if (ix.programId !== FLASHX_PROGRAM || !Buffer.isBuffer(ix.data)) continue;
      if (ix.data.length === 1 && ix.data[0] === 5 && ix.accountIndices.length >= 6) {
        ataMint = keyAt(ix.accountIndices[3]);
        ataTokenProgram = keyAt(ix.accountIndices[5]);
      }
    }
    for (const ix of rawTx.instructions || []) {
      if (ix.programId !== FLASHX_PROGRAM || !Buffer.isBuffer(ix.data) || ix.data.length < 18) continue;
      const ixAccounts = ix.accountIndices.map(keyAt);
      if (!ixAccounts.includes(PUMP_PROGRAM)) continue;
      // In FLASHX Pump.fun buy route: data[0] === 0, accounts[7] = mint, accounts[8] = quoteMint (WSOL), accounts[9] = tokenProgram
      const mint = ataMint || ixAccounts[7] || ixAccounts.find((a) => a && /pump$/i.test(a));
      const quoteMint = ixAccounts[8] || WSOL_MINT;
      const tokenProgram = ataTokenProgram || ixAccounts[9] || detectedTokenProgram;
      if (!mint || mint === WSOL_MINT) continue;
      const firstU64 = ix.data.readBigUInt64LE(1);
      const secondU64 = ix.data.readBigUInt64LE(10);
      if (ix.data[0] === 0 && firstU64 > 0n && quoteMint === WSOL_MINT) {
        const solAmt = Number(firstU64) / LAMPORTS_PER_SOL;
        const tokAmt = secondU64 > 0n ? Number(secondU64) / 1e6 : 1;
        if (solAmt > 0 && solAmt < 1000) {
          return {
            side: 'buy',
            sellSharePct: null,
            tokenMint: mint,
            tokenDecimals: 6,
            tokenAmount: tokAmt,
            quoteSymbol: 'SOL',
            quoteMint: WSOL_MINT,
            quoteAmount: solAmt,
            poolQuoteAmount: solAmt,
            priceInQuote: tokAmt > 0 ? solAmt / tokAmt : 0,
            venue: 'pumpfun',
            signature: rawTx.signature,
            slot,
            blockTime: Date.now(),
            curve: { tokenProgram, at: Date.now() },
            preExecution: true,
          };
        }
      }
    }
  }

  return null;
}

// ---------- LocalShredFeed Client ----------

export class LocalShredFeed {
  /**
   * @param url       Upstream LocalShred Lite endpoint (`http://localshred-lite.nlams.dedicatednodes.io:9999`)
   * @param apiKey    LocalShred Lite API key (`z4UpUpL-kjwUKiR-8eDgYr`), sent in `x-localshred-auth`
   * @param proxyUrl  Optional local `shredstream-proxy` gRPC service URL (`http://127.0.0.1:10202`) for `SubscribeEntries`
   * @param udpPort   Local UDP port to bind for incoming raw data shreds in direct heartbeat mode (default 20000)
   * @param udpHost   IP sent in the `Heartbeat.socket.ip` message (default `'0.0.0.0'`)
   * @param onTx      `onTx(walletAddress, { signature, slot, receivedAt, rawTx, preParsedSwap })`
   */
  constructor({
    url = DEFAULT_LOCALSHRED_URL,
    apiKey = null,
    proxyUrl = null,
    udpPort = 20000,
    udpHost = '0.0.0.0',
    regions = [],
    onTx = () => {},
    onChange = () => {},
    redactUrl = (s) => s,
    connect = http2.connect,
    createSocket = () => dgram.createSocket('udp4'),
    maxBackoffMs = 30_000,
  } = {}) {
    const rawUrl = String(url || DEFAULT_LOCALSHRED_URL).trim();
    const u = new URL(rawUrl);
    this.apiKey = apiKey || u.searchParams.get('api-key') || u.searchParams.get('x-localshred-auth') || null;
    u.search = '';
    this.origin = u.origin;
    this.proxyUrl = proxyUrl ? String(proxyUrl).trim() : null;
    this.udpPort = Number.isInteger(Number(udpPort)) && Number(udpPort) >= 0 ? Number(udpPort) : 20000;
    this.udpHost = String(udpHost || '0.0.0.0');
    this.regions = Array.isArray(regions) ? regions.filter(Boolean) : [];
    this.onTx = onTx;
    this.onChange = onChange;
    this.redactUrl = redactUrl;
    this.connect = connect;
    this.createSocket = createSocket;
    this.maxBackoffMs = maxBackoffMs;

    this.wallets = new Set();
    this.running = false;
    this.session = null;
    this.proxySession = null;
    this.proxyStream = null;
    this.udpSocket = null;
    this.boundUdpPort = null;
    this.heartbeatTimer = null;
    this.retryTimer = null;
    this.backoff = 1000;
    this.assembler = new DataShredAssembler();
    this.seenSigs = new Set();

    this.stats = {
      connected: false,
      mode: this.proxyUrl ? 'proxy-entries' : 'direct-udp',
      connects: 0,
      heartbeats: 0,
      ttlMs: null,
      udpPort: this.udpPort,
      shredsReceived: 0,
      entriesDecoded: 0,
      txsScanned: 0,
      txsMatched: 0,
      preParsedSwaps: 0,
      bytes: 0,
      lastHeartbeatAt: null,
      lastShredAt: null,
      lastTxAt: null,
      lastError: null,
      lastErrorAt: null,
    };
  }

  status() {
    return {
      kind: 'localshred-lite',
      url: this.redactUrl(this.proxyUrl || this.origin),
      upstreamUrl: this.redactUrl(this.origin),
      hasApiKey: Boolean(this.apiKey),
      wallets: this.wallets.size,
      ...this.stats,
    };
  }

  setWallets(addresses) {
    this.wallets = new Set(addresses);
  }

  start() {
    if (this.running) return;
    this.running = true;
    if (this.proxyUrl) {
      this.openProxyStream();
    } else {
      this.startDirectUdpAndHeartbeat();
    }
  }

  stop() {
    this.running = false;
    clearTimeout(this.retryTimer);
    clearTimeout(this.heartbeatTimer);
    try {
      this.proxyStream?.close();
    } catch {
      // ignore
    }
    try {
      this.proxySession?.close();
    } catch {
      // ignore
    }
    try {
      this.session?.close();
    } catch {
      // ignore
    }
    try {
      this.udpSocket?.close();
    } catch {
      // ignore
    }
    this.proxyStream = null;
    this.proxySession = null;
    this.session = null;
    this.udpSocket = null;
    this.boundUdpPort = null;
    this.stats.connected = false;
    this.onChange();
  }

  fail(why) {
    const clean = this.redactUrl(String(why || 'unknown error')).slice(0, 240);
    this.stats.lastError = clean;
    this.stats.lastErrorAt = Date.now();
    this.stats.connected = false;
    clearTimeout(this.heartbeatTimer);
    try {
      this.proxySession?.destroy();
    } catch {
      // ignore
    }
    try {
      this.session?.destroy();
    } catch {
      // ignore
    }
    this.proxySession = null;
    this.proxyStream = null;
    this.session = null;
    this.onChange();
    if (!this.running) return;
    clearTimeout(this.retryTimer);
    this.retryTimer = setTimeout(() => {
      if (!this.running) return;
      if (this.proxyUrl) this.openProxyStream();
      else this.sendHeartbeatOnce();
    }, this.backoff);
    this.retryTimer.unref?.();
    this.backoff = Math.min(this.maxBackoffMs, this.backoff * 2);
  }

  // ---------- Mode 1: Direct Heartbeat + UDP Data Shred Receiver ----------

  startDirectUdpAndHeartbeat() {
    if (!this.running) return;
    if (!this.udpSocket) {
      try {
        const sock = this.createSocket();
        this.udpSocket = sock;
        sock.on('error', (err) => {
          this.stats.lastError = `UDP error: ${err.message}`;
          this.stats.lastErrorAt = Date.now();
          this.onChange();
        });
        sock.on('message', (msg) => {
          this.stats.shredsReceived += 1;
          this.stats.bytes += msg.length;
          this.stats.lastShredAt = Date.now();
          const batches = this.assembler.pushPacket(msg);
          for (const b of batches) {
            this.stats.entriesDecoded += 1;
            this.handleDecodedTransactions(b.txs, b.slot);
          }
        });
        sock.bind(this.udpPort, () => {
          try {
            const addr = sock.address();
            this.boundUdpPort = addr?.port || this.udpPort;
            this.stats.udpPort = this.boundUdpPort;
          } catch {
            this.boundUdpPort = this.udpPort;
          }
          sock.unref?.();
          this.sendHeartbeatOnce();
        });
        return;
      } catch (err) {
        return this.fail(`UDP bind failed: ${err.message}`);
      }
    }
    this.sendHeartbeatOnce();
  }

  sendHeartbeatOnce() {
    if (!this.running) return;
    let session = this.session;
    if (!session || session.closed || session.destroyed) {
      try {
        session = this.connect(this.origin);
        this.session = session;
        session.on('error', (err) => this.fail(err.message));
        session.on('goaway', () => {
          this.session = null;
        });
        session.unref?.();
      } catch (err) {
        return this.fail(err.message);
      }
    }

    const headers = {
      ':method': 'POST',
      ':path': '/shredstream.Shredstream/SendHeartbeat',
      'content-type': 'application/grpc',
      te: 'trailers',
    };
    if (this.apiKey) {
      headers['x-localshred-auth'] = this.apiKey;
    }

    let req;
    try {
      req = session.request(headers);
    } catch (err) {
      this.session = null;
      return this.fail(err.message);
    }

    let respHeaders = null;
    let trailers = null;
    let dataBuf = Buffer.alloc(0);

    req.on('response', (h) => {
      respHeaders = h;
    });
    req.on('data', (chunk) => {
      dataBuf = dataBuf.length ? Buffer.concat([dataBuf, chunk]) : chunk;
    });
    req.on('trailers', (t) => {
      trailers = t;
    });
    req.on('error', (err) => {
      this.fail(err.message);
    });
    req.on('end', () => {
      const status = trailers?.['grpc-status'] ?? respHeaders?.['grpc-status'];
      const msg = trailers?.['grpc-message'] ?? respHeaders?.['grpc-message'];
      if (status !== undefined && String(status) !== '0') {
        const decodedMsg = msg ? decodeURIComponent(String(msg)) : 'heartbeat refused';
        return this.fail(`gRPC ${status}: ${decodedMsg}`);
      }
      let ttlMs = 1000;
      if (dataBuf.length >= 5) {
        const len = dataBuf.readUInt32BE(1);
        if (dataBuf.length >= 5 + len) {
          try {
            const parsed = decodeHeartbeatResponse(dataBuf.subarray(5, 5 + len));
            if (parsed.ttlMs > 0) ttlMs = parsed.ttlMs;
          } catch {
            // keep default 1000ms
          }
        }
      }
      const wasConnected = this.stats.connected;
      this.stats.connected = true;
      if (!wasConnected) this.stats.connects += 1;
      this.stats.heartbeats += 1;
      this.stats.ttlMs = ttlMs;
      this.stats.lastHeartbeatAt = Date.now();
      this.stats.lastError = null;
      this.backoff = 1000;
      if (!wasConnected) this.onChange();

      clearTimeout(this.heartbeatTimer);
      const nextMs = Math.max(250, Math.min(1000, Math.floor(ttlMs / 3)));
      this.heartbeatTimer = setTimeout(() => this.sendHeartbeatOnce(), nextMs);
      this.heartbeatTimer.unref?.();
    });

    const port = this.boundUdpPort || this.udpPort || 20000;
    const hb = encodeHeartbeat({ ip: this.udpHost, port, regions: this.regions });
    req.end(grpcFrame(hb));
  }

  // ---------- Mode 2: Proxy gRPC `SubscribeEntries` Stream ----------

  openProxyStream() {
    if (!this.running || !this.proxyUrl) return;
    let session;
    try {
      const u = new URL(this.proxyUrl);
      session = this.connect(u.origin);
    } catch (err) {
      return this.fail(err.message);
    }
    this.proxySession = session;
    session.on('error', (err) => this.fail(err.message));
    session.on('goaway', () => this.fail('proxy closed the connection'));

    const headers = {
      ':method': 'POST',
      ':path': '/shredstream.ShredstreamProxy/SubscribeEntries',
      'content-type': 'application/grpc',
      te: 'trailers',
    };
    if (this.apiKey) {
      headers['x-localshred-auth'] = this.apiKey;
    }

    const stream = session.request(headers);
    this.proxyStream = stream;
    let buf = Buffer.alloc(0);

    stream.on('response', (h) => {
      const status = h['grpc-status'];
      if (status !== undefined && String(status) !== '0') {
        return this.fail(`gRPC ${status}: ${decodeURIComponent(String(h['grpc-message'] || 'refused'))}`);
      }
      this.stats.connected = true;
      this.stats.connects += 1;
      this.stats.lastError = null;
      this.backoff = 1000;
      this.onChange();
    });

    stream.on('data', (chunk) => {
      this.stats.bytes += chunk.length;
      buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
      while (buf.length >= 5) {
        const len = buf.readUInt32BE(1);
        if (buf.length < 5 + len) break;
        const msg = buf.subarray(5, 5 + len);
        buf = buf.subarray(5 + len);
        this.handleEntryMessage(msg);
      }
    });

    stream.on('trailers', (h) => {
      const status = h['grpc-status'];
      if (status !== undefined && String(status) !== '0') {
        this.fail(`gRPC ${status}: ${decodeURIComponent(String(h['grpc-message'] || 'stream ended'))}`);
      }
    });
    stream.on('error', (err) => this.fail(err.message));
    stream.on('close', () => {
      if (this.proxyStream === stream) this.fail('stream closed');
    });

    // SubscribeEntriesRequest is empty `{}`
    stream.end(grpcFrame(Buffer.alloc(0)));
  }

  handleEntryMessage(msg) {
    let decoded;
    try {
      decoded = decodeEntryMessage(msg);
    } catch {
      return;
    }
    if (!decoded.entriesBuf?.length) return;
    this.stats.entriesDecoded += 1;
    this.stats.lastShredAt = Date.now();
    let txs;
    try {
      txs = decodeBincodeEntries(decoded.entriesBuf);
    } catch {
      return;
    }
    if (txs.length) {
      this.handleDecodedTransactions(txs, decoded.slot);
    }
  }

  // ---------- Transaction Matching & Pre-Execution Swap Extraction ----------

  handleDecodedTransactions(txs, slot) {
    if (!Array.isArray(txs) || !txs.length) return;
    this.stats.txsScanned += txs.length;
    if (!this.wallets.size) return;

    const receivedAt = Date.now();
    for (const rawTx of txs) {
      if (!rawTx?.signature || !Array.isArray(rawTx.staticAccountKeys)) continue;
      // Match watched wallets that appear in the transaction's account keys (or signers)
      const matchedWallets = [];
      for (const k of rawTx.staticAccountKeys) {
        if (this.wallets.has(k)) matchedWallets.push(k);
      }
      if (!matchedWallets.length) continue;

      if (this.seenSigs.has(rawTx.signature)) continue;
      this.seenSigs.add(rawTx.signature);
      if (this.seenSigs.size > 4000) {
        this.seenSigs = new Set([...this.seenSigs].slice(-2000));
      }

      this.stats.txsMatched += 1;
      this.stats.lastTxAt = receivedAt;

      for (const wallet of matchedWallets) {
        const preParsedSwap = extractSwapFromRawTx(rawTx, wallet, { slot });
        if (preParsedSwap) this.stats.preParsedSwaps += 1;
        try {
          this.onTx(wallet, {
            signature: rawTx.signature,
            slot,
            receivedAt,
            rawTx,
            preParsedSwap,
          });
        } catch {
          // listener errors never break the feed
        }
      }
    }
  }
}
