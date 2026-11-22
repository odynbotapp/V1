// LocalShred Lite & ShredStream live feed:
//   - Heartbeat protobuf encoder/decoder + x-localshred-auth gRPC SendHeartbeat
//   - Raw Solana UDP data shred parser + per-slot contiguous batch reassembler (DataShredAssembler)
//   - Bincode Vec<Entry> + VersionedTransaction decoder (legacy & v0)
//   - Pre-execution Pump.fun direct & FLASHX instruction swap extractor (extractSwapFromRawTx)
//   - Local proxy gRPC SubscribeEntries stream
//   - SolanaWalletWatcher integration (fastDetect pre-execution trade & feedRace.shred)
// Run: node --test test/localshred.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import http2 from 'node:http2';
import dgram from 'node:dgram';
import crypto from 'node:crypto';
import {
  LocalShredFeed,
  DataShredAssembler,
  encodeHeartbeat,
  decodeHeartbeatResponse,
  decodeEntryMessage,
  decodeVersionedTransactionAt,
  decodeBincodeEntries,
  parseDataShred,
  extractSwapFromRawTx,
  grpcFrame,
  PUMP_PROGRAM,
  FLASHX_PROGRAM,
  WSOL_MINT,
  TOKEN_2022_PROGRAM,
} from '../engine/localshred.mjs';
import { pbFields } from '../engine/yellowstone.mjs';
import { base58Decode, base58Encode } from '../engine/onchainMeta.mjs';
import { compileMessage, buyExactQuoteInIx } from '../engine/pumpDirect.mjs';
import { SolanaWalletWatcher } from '../engine/walletWatcher.mjs';

const DECU = '4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9';
const MINT = 'ELS8zfWCnFeU7H9KbU1uki2pVFgDTqtH6k8FGQy4pump';
const BLOCKHASH = '4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi';

// Helper to build a serialized legacy transaction buffer from instructions
function buildRawLegacyTx({ payer = DECU, instructions = [], recentBlockhash = BLOCKHASH }) {
  const { message } = compileMessage({ payer, instructions, recentBlockhash });
  const sig = crypto.randomBytes(64);
  return {
    rawBytes: Buffer.concat([Buffer.from([1]), sig, message]),
    signature: base58Encode(sig),
  };
}

// Helper to wrap serialized transaction buffers into a bincode Vec<Entry> buffer
function buildBincodeEntries(rawTxBuffers) {
  const entryCount = Buffer.alloc(8);
  entryCount.writeBigUInt64LE(1n, 0);
  const numHashes = Buffer.alloc(8);
  numHashes.writeBigUInt64LE(64n, 0);
  const hash = crypto.randomBytes(32);
  const txCount = Buffer.alloc(8);
  txCount.writeBigUInt64LE(BigInt(rawTxBuffers.length), 0);
  return Buffer.concat([entryCount, numHashes, hash, txCount, ...rawTxBuffers]);
}

// Helper to wrap a payload slice into a raw Solana Merkle Data Shred packet
function buildDataShredPacket({ slot = 465_000_000, index = 0, dataComplete = false, lastInSlot = false, payload }) {
  const totalSize = 88 + payload.length;
  const pkt = Buffer.alloc(totalSize + 64); // extra room for merkle proof trailer after totalSize
  crypto.randomBytes(64).copy(pkt, 0); // leader signature
  pkt[64] = 0x95; // Merkle Data Shred (high two bits 10 = 0x80)
  pkt.writeBigUInt64LE(BigInt(slot), 65);
  pkt.writeUInt32LE(index, 73);
  pkt.writeUInt16LE(50093, 77); // shred version
  pkt.writeUInt32LE(0, 79); // fec_set_index
  pkt.writeUInt16LE(1, 83); // parent_offset
  pkt[85] = lastInSlot ? 0xc0 : dataComplete ? 0x40 : 0x00;
  pkt.writeUInt16LE(totalSize, 86);
  payload.copy(pkt, 88);
  return pkt;
}

test('LS1 Heartbeat protobuf codec: encodes Socket { ip, port } + regions and decodes HeartbeatResponse { ttl_ms }', () => {
  const hb = encodeHeartbeat({ ip: '0.0.0.0', port: 20000, regions: ['nlams', 'deffm'] });
  const fields = pbFields(hb);
  const sockField = fields.find((f) => f.field === 1);
  assert.ok(sockField);
  const sock = pbFields(sockField.value);
  assert.equal(sock.find((f) => f.field === 1).value.toString('utf8'), '0.0.0.0');
  assert.equal(Number(sock.find((f) => f.field === 2).value), 20000);
  const regs = fields.filter((f) => f.field === 2).map((f) => f.value.toString('utf8'));
  assert.deepEqual(regs, ['nlams', 'deffm']);

  // HeartbeatResponse { ttl_ms = 1500 } -> tag 0x08, varint 1500 (0xdc, 0x0b)
  const resp = decodeHeartbeatResponse(Buffer.from([0x08, 0xdc, 0x0b]));
  assert.equal(resp.ttlMs, 1500);
});

test('LS2 Bincode Vec<Entry> + VersionedTransaction decoder & direct Pump.fun buy_exact_quote_in_v3 pre-execution swap extraction', () => {
  const buyIx = buyExactQuoteInIx({
    user: DECU,
    mint: MINT,
    baseTokenProgram: TOKEN_2022_PROGRAM,
    lamportsIn: 2_500_000_000n, // 2.5 SOL
    minTokensOut: 55_000_000_000_000n, // 55M tokens (6 decimals)
  });
  const { rawBytes, signature } = buildRawLegacyTx({ payer: DECU, instructions: [buyIx] });
  const entriesBuf = buildBincodeEntries([rawBytes]);

  const txs = decodeBincodeEntries(entriesBuf);
  assert.equal(txs.length, 1);
  assert.equal(txs[0].signature, signature);
  assert.ok(txs[0].signers.includes(DECU));
  assert.ok(txs[0].staticAccountKeys.includes(MINT));

  const swap = extractSwapFromRawTx(txs[0], DECU, { slot: 465_100_000 });
  assert.ok(swap, 'pre-execution swap extracted from raw instruction');
  assert.equal(swap.side, 'buy');
  assert.equal(swap.tokenMint, MINT);
  assert.equal(swap.quoteSymbol, 'SOL');
  assert.equal(swap.quoteAmount, 2.5);
  assert.equal(swap.tokenAmount, 55_000_000);
  assert.equal(swap.venue, 'pumpfun');
  assert.equal(swap.curve.tokenProgram, TOKEN_2022_PROGRAM);
  assert.equal(swap.preExecution, true);
});

test('LS3 Pre-execution FLASHX + Pump.fun buy extraction (matches @Decu live transaction layout)', () => {
  // @Decu's FLASHX route:
  // ix #1: FLASHX ATA creation (data: [5], 7 accounts where accounts[3] = mint, accounts[5] = tokenProgram)
  // ix #2: FLASHX Pump.fun buy (data: [0, u64LE(lamportsIn), 1, u64LE(minTokensOut), ...], accounts include PUMP_PROGRAM, accounts[7] = mint, accounts[8] = WSOL, accounts[9] = tokenProgram)
  const meta = (pubkey, isSigner = false, isWritable = true) => ({ pubkey, isSigner, isWritable });
  const dummy = (i) => base58Encode(Buffer.alloc(32, i + 2));
  const ataIx = {
    programId: FLASHX_PROGRAM,
    keys: [
      meta(DECU, true, true),
      meta(dummy(1)),
      meta(DECU, true, true),
      meta(MINT, false, false),
      meta('11111111111111111111111111111111', false, false),
      meta(TOKEN_2022_PROGRAM, false, false),
      meta('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', false, false),
    ],
    data: Buffer.from([5]),
  };
  const swapData = Buffer.alloc(22);
  swapData[0] = 0; // buy
  swapData.writeBigUInt64LE(2_970_000_000n, 1); // 2.97 SOL
  swapData[9] = 1;
  swapData.writeBigUInt64LE(66_072_768_286_978n, 10); // 66,072,768.286978 tokens
  const swapIx = {
    programId: FLASHX_PROGRAM,
    keys: [
      meta(DECU, true, true),
      meta(dummy(2)),
      meta(dummy(3)),
      meta(dummy(4)),
      meta(dummy(5)),
      meta(PUMP_PROGRAM, false, false),
      meta(dummy(6)),
      meta(MINT, false, false),
      meta(WSOL_MINT, false, false),
      meta(TOKEN_2022_PROGRAM, false, false),
    ],
    data: swapData,
  };
  const { rawBytes, signature } = buildRawLegacyTx({ payer: DECU, instructions: [ataIx, swapIx] });
  const { tx } = decodeVersionedTransactionAt(rawBytes, 0);
  const swap = extractSwapFromRawTx(tx, DECU, { slot: 465_200_000 });
  assert.ok(swap);
  assert.equal(swap.signature, signature);
  assert.equal(swap.side, 'buy');
  assert.equal(swap.tokenMint, MINT);
  assert.equal(swap.quoteAmount, 2.97);
  assert.equal(Number(swap.tokenAmount.toFixed(2)), 66072768.29);
  assert.equal(swap.curve.tokenProgram, TOKEN_2022_PROGRAM);
});

test('LS4 DataShredAssembler: reassembles multi-shred UDP batch (out-of-order arrival) into decoded transactions', () => {
  const buyIx = buyExactQuoteInIx({
    user: DECU,
    mint: MINT,
    baseTokenProgram: TOKEN_2022_PROGRAM,
    lamportsIn: 1_500_000_000n,
    minTokensOut: 30_000_000_000_000n,
  });
  const { rawBytes, signature } = buildRawLegacyTx({ payer: DECU, instructions: [buyIx] });
  const entriesBuf = buildBincodeEntries([rawBytes]);

  // Split entriesBuf across 2 UDP data shreds (index 0 and index 1 with dataComplete = true)
  const mid = Math.floor(entriesBuf.length / 2);
  const pkt0 = buildDataShredPacket({ slot: 465_333_000, index: 0, dataComplete: false, payload: entriesBuf.subarray(0, mid) });
  const pkt1 = buildDataShredPacket({ slot: 465_333_000, index: 1, dataComplete: true, payload: entriesBuf.subarray(mid) });

  assert.ok(parseDataShred(pkt0));
  assert.equal(parseDataShred(pkt0).dataComplete, false);
  assert.equal(parseDataShred(pkt1).dataComplete, true);

  const assembler = new DataShredAssembler();
  // Deliver shred 1 first (out of order), then shred 0
  assert.deepEqual(assembler.pushPacket(pkt1), []);
  const batches = assembler.pushPacket(pkt0);
  assert.equal(batches.length, 1);
  assert.equal(batches[0].slot, 465_333_000);
  assert.equal(batches[0].txs.length, 1);
  assert.equal(batches[0].txs[0].signature, signature);
});

test('LS5 Live Direct Heartbeat + UDP Data Shred flow over HTTP/2 + dgram', async () => {
  const seenHeaders = [];
  const heartbeatRequests = [];
  let clientUdpPort = null;

  const server = http2.createServer();
  server.on('stream', (stream, headers) => {
    seenHeaders.push(headers);
    let buf = Buffer.alloc(0);
    stream.on('data', (c) => {
      buf = Buffer.concat([buf, c]);
    });
    stream.on('end', () => {
      if (buf.length >= 5) {
        const len = buf.readUInt32BE(1);
        const hbFields = pbFields(buf.subarray(5, 5 + len));
        const sockFields = pbFields(hbFields.find((f) => f.field === 1).value);
        clientUdpPort = Number(sockFields.find((f) => f.field === 2).value);
        heartbeatRequests.push({ port: clientUdpPort });
      }
      stream.respond({ ':status': 200, 'content-type': 'application/grpc' }, { waitForTrailers: true });
      stream.on('wantTrailers', () => {
        stream.sendTrailers({ 'grpc-status': '0' });
      });
      // HeartbeatResponse { ttl_ms = 1200 }
      stream.end(grpcFrame(Buffer.from([0x08, 0xb0, 0x09])));
    });
  });

  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const h2Port = server.address().port;

  const got = [];
  const feed = new LocalShredFeed({
    url: `http://127.0.0.1:${h2Port}`,
    apiKey: 'z4UpUpL-kjwUKiR-8eDgYr',
    udpPort: 0, // ephemeral UDP port
    onTx: (wallet, ev) => got.push({ wallet, ...ev }),
  });
  feed.setWallets([DECU]);
  feed.start();

  const until = async (fn, ms = 3000) => {
    const t0 = Date.now();
    while (!fn()) {
      if (Date.now() - t0 > ms) throw new Error('timed out');
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  await until(() => feed.stats.connected && clientUdpPort > 0);
  assert.equal(seenHeaders[0][':path'], '/shredstream.Shredstream/SendHeartbeat');
  assert.equal(seenHeaders[0]['x-localshred-auth'], 'z4UpUpL-kjwUKiR-8eDgYr');
  assert.equal(feed.stats.ttlMs, 1200);

  // Send a raw UDP data shred containing a Pump.fun buy signed by DECU to clientUdpPort
  const buyIx = buyExactQuoteInIx({
    user: DECU,
    mint: MINT,
    baseTokenProgram: TOKEN_2022_PROGRAM,
    lamportsIn: 2_000_000_000n,
    minTokensOut: 40_000_000_000_000n,
  });
  const { rawBytes, signature } = buildRawLegacyTx({ payer: DECU, instructions: [buyIx] });
  const entriesBuf = buildBincodeEntries([rawBytes]);
  const pkt = buildDataShredPacket({ slot: 465_444_000, index: 0, dataComplete: true, payload: entriesBuf });

  const sender = dgram.createSocket('udp4');
  sender.send(pkt, clientUdpPort, '127.0.0.1');

  await until(() => got.length === 1);
  assert.equal(got[0].wallet, DECU);
  assert.equal(got[0].signature, signature);
  assert.equal(got[0].slot, 465_444_000);
  assert.ok(got[0].preParsedSwap);
  assert.equal(got[0].preParsedSwap.tokenMint, MINT);
  assert.equal(got[0].preParsedSwap.quoteAmount, 2);

  sender.close();
  feed.stop();
  server.close();
});

test('LS6 Proxy gRPC SubscribeEntries stream + SolanaWalletWatcher pre-execution fast trade integration', async () => {
  const buyIx = buyExactQuoteInIx({
    user: DECU,
    mint: MINT,
    baseTokenProgram: TOKEN_2022_PROGRAM,
    lamportsIn: 3_000_000_000n,
    minTokensOut: 60_000_000_000_000n,
  });
  const { rawBytes, signature } = buildRawLegacyTx({ payer: DECU, instructions: [buyIx] });
  const entriesBuf = buildBincodeEntries([rawBytes]);

  // Build protobuf Entry { slot = 465_555_000, entries = entriesBuf }
  const varint = (n) => {
    let v = BigInt(n);
    const o = [];
    do {
      let b = Number(v & 0x7fn);
      v >>= 7n;
      if (v) b |= 0x80;
      o.push(b);
    } while (v);
    return Buffer.from(o);
  };
  const entryMsg = Buffer.concat([
    Buffer.from([0x08]),
    varint(465_555_000),
    Buffer.from([0x12]),
    varint(entriesBuf.length),
    entriesBuf,
  ]);

  const server = http2.createServer();
  server.on('stream', (stream, headers) => {
    assert.equal(headers[':path'], '/shredstream.ShredstreamProxy/SubscribeEntries');
    stream.respond({ ':status': 200, 'content-type': 'application/grpc' });
    stream.write(grpcFrame(entryMsg));
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  const proxyPort = server.address().port;

  const signals = [];
  const watcher = new SolanaWalletWatcher({
    rpcUrl: 'http://127.0.0.1:9',
    stream: true,
    shredProxyUrl: `http://127.0.0.1:${proxyPort}`,
    shredApiKey: 'z4UpUpL-kjwUKiR-8eDgYr',
    tokenInfo: {
      cache: new Map([[MINT, { at: Date.now(), info: { symbol: 'SHRED', name: 'Shred Token', priceUsd: 0.00001 } }]]),
      ttlMs: 60_000,
      get: async () => ({ symbol: 'SHRED', name: 'Shred Token', priceUsd: 0.00001 }),
      solPriceFast: async () => 150,
      solPriceUsd: async () => 150,
    },
    onSignal: (s) => signals.push(s),
  });
  watcher.fastDetect = () => true;
  watcher.confirmDelaysMs = []; // skip background RPC poll in unit test
  watcher.setWallets([{ address: DECU, label: '@Decu' }]);
  watcher.running = true;
  watcher.shredFeed.setWallets([DECU]);
  watcher.shredFeed.start();

  const until = async (fn, ms = 3000) => {
    const t0 = Date.now();
    while (!fn()) {
      if (Date.now() - t0 > ms) throw new Error('timed out');
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  await until(() => signals.length === 1);
  assert.equal(signals[0].signature, signature);
  assert.equal(signals[0].wallet, DECU);
  assert.equal(signals[0].side, 'buy');
  assert.equal(signals[0].tokenMint, MINT);
  assert.equal(signals[0].quoteAmount, 3);
  assert.equal(watcher.feedRace.shred, 1);
  assert.equal(watcher.getStatus().stream.shredFeed.firstShred, 1);

  watcher.shredFeed.stop();
  watcher.running = false;
  server.close();
  server.emit('close');
});
