import { PgStore } from '../../src/pg/pg-store';
import * as fs from 'node:fs';
import * as readline from 'node:readline/promises';
import * as zlib from 'node:zlib';
import { EventStreamHandler } from '../../src/event-stream/event-stream';
import { onceWhen } from '@hirosystems/api-toolkit';

describe('End-to-end ingestion tests', () => {
  let snpObserverUrl: string;

  const sampleEventsLastMsgId = '5402-0'; // last msgID in the stackerdb-sample-events.tsv.gz events dump
  const sampleEventsBlockHeight = 505; // last block height in the stackerdb-sample-events.tsv.gz events dump

  let db: PgStore;
  beforeAll(async () => {
    snpObserverUrl = process.env['SNP_OBSERVER_URL'] as string;
    db = await PgStore.connect();
  });

  afterAll(async () => {
    await db.close();
  });

  test('db chaintip starts at 1', async () => {
    const chainTip = await db.getChainTip(db.sql);
    expect(chainTip.block_height).toBe(1);
  });

  test('populate SNP server data', async () => {
    const payloadDumpFile = './tests/dumps/stackerdb-sample-events.tsv.gz';
    const rl = readline.createInterface({
      input: fs.createReadStream(payloadDumpFile).pipe(zlib.createGunzip()),
      crlfDelay: Infinity,
    });
    for await (const line of rl) {
      const [_id, timestamp, path, payload] = line.split('\t');
      // use fetch to POST the payload to the SNP event observer server
      try {
        const res = await fetch(snpObserverUrl + path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Original-Timestamp': timestamp },
          body: payload,
        });
        if (res.status !== 200) {
          throw new Error(`Failed to POST event: ${path} - ${payload.slice(0, 100)}`);
        }
      } catch (error) {
        console.error(`Error posting event: ${error}`, error);
        throw error;
      }
    }
    rl.close();
  });

  test('ingest msgs from SNP server', async () => {
    const eventStreamListener = new EventStreamHandler({ db });
    await eventStreamListener.start();
    // wait for last msgID to be processed
    const [{ msgId: lastMsgProcessed }] = await onceWhen(
      eventStreamListener.events,
      'processedMessage',
      ({ msgId }) => {
        return msgId === sampleEventsLastMsgId;
      }
    );
    expect(lastMsgProcessed).toBe(sampleEventsLastMsgId);
    await eventStreamListener.stop();
  });

  test('validate blocks ingested', async () => {
    const chainTip = await db.getChainTip(db.sql);
    expect(chainTip.block_height).toBe(sampleEventsBlockHeight);
  });

  test('validate cycle signer data', async () => {
    const signerData = await db.getSignersForCycle({
      sql: db.sql,
      cycleNumber: 6,
      limit: 10,
      offset: 0,
    });
    expect(signerData).toBeDefined();
    expect(signerData.length).toBe(3);

    expect(signerData[0].signer_key).toBe(
      '0x028efa20fa5706567008ebaf48f7ae891342eeb944d96392f719c505c89f84ed8d'
    );
    expect(signerData[0].last_metadata_server_version).toBe(
      'stacks-signer 0.0.1 (:dd1ebe64603f54dae48558a5d82d9bd885e97a01, debug build, linux [aarch64])'
    );
    expect(signerData[0].stacked_amount).toBe('4125240000000000');
    expect(signerData[0].slot_index).toBe(1);
    expect(signerData[0].stacked_amount_percentage).toBe(50);
    expect(signerData[0].stacked_amount_rank).toBe(1);
    expect(signerData[0].weight).toBe(4);
    expect(signerData[0].weight_percentage).toBe(50);

    expect(signerData[1].signer_key).toBe(
      '0x023f19d77c842b675bd8c858e9ac8b0ca2efa566f17accf8ef9ceb5a992dc67836'
    );
    expect(signerData[1].last_metadata_server_version).toBe(
      'stacks-signer 0.0.1 (:dd1ebe64603f54dae48558a5d82d9bd885e97a01, debug build, linux [aarch64])'
    );
    expect(signerData[1].stacked_amount).toBe('2750160000000000');
    expect(signerData[1].slot_index).toBe(0);
    expect(signerData[1].stacked_amount_percentage).toBe(33.333);
    expect(signerData[1].stacked_amount_rank).toBe(2);
    expect(signerData[1].weight).toBe(3);
    expect(signerData[1].weight_percentage).toBe(37.5);

    expect(signerData[2].signer_key).toBe(
      '0x029fb154a570a1645af3dd43c3c668a979b59d21a46dd717fd799b13be3b2a0dc7'
    );
    expect(signerData[2].last_metadata_server_version).toBe(
      'stacks-signer 0.0.1 (:dd1ebe64603f54dae48558a5d82d9bd885e97a01, debug build, linux [aarch64])'
    );
    expect(signerData[2].stacked_amount).toBe('1375080000000000');
    expect(signerData[2].slot_index).toBe(2);
    expect(signerData[2].stacked_amount_percentage).toBe(16.667);
    expect(signerData[2].stacked_amount_rank).toBe(3);
    expect(signerData[2].weight).toBe(1);
    expect(signerData[2].weight_percentage).toBe(12.5);
  });

  test('validate cycle single signer data', async () => {
    const signerData = await db.getSignerForCycle(
      6,
      '0x028efa20fa5706567008ebaf48f7ae891342eeb944d96392f719c505c89f84ed8d'
    );
    expect(signerData).toBeDefined();
    expect(signerData?.slot_index).toBe(1);
    expect(signerData?.stacked_amount).toBe('4125240000000000');
    expect(signerData?.stacked_amount_percentage).toBe(50);
    expect(signerData?.stacked_amount_rank).toBe(1);
    expect(signerData?.weight).toBe(4);
    expect(signerData?.weight_percentage).toBe(50);
    expect(signerData?.signer_key).toBe(
      '0x028efa20fa5706567008ebaf48f7ae891342eeb944d96392f719c505c89f84ed8d'
    );
  });

  test('validate current cycle signer weight percentages', async () => {
    const signerWeightPercentage = await db.getCurrentCycleSignersWeightPercentage();
    expect(signerWeightPercentage).toBeDefined();
    expect(signerWeightPercentage.length).toBe(3);
    expect(signerWeightPercentage[0].signer_key).toBe(
      '0x028efa20fa5706567008ebaf48f7ae891342eeb944d96392f719c505c89f84ed8d'
    );
    expect(signerWeightPercentage[0].weight).toBe(50);
    expect(signerWeightPercentage[1].signer_key).toBe(
      '0x023f19d77c842b675bd8c858e9ac8b0ca2efa566f17accf8ef9ceb5a992dc67836'
    );
    expect(signerWeightPercentage[1].weight).toBe(37.5);
    expect(signerWeightPercentage[2].signer_key).toBe(
      '0x029fb154a570a1645af3dd43c3c668a979b59d21a46dd717fd799b13be3b2a0dc7'
    );
    expect(signerWeightPercentage[2].weight).toBe(12.5);
  });
});
