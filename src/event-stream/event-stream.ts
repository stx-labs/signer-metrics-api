import { PgStore } from '../pg/pg-store';
import { logger as defaultLogger, stopwatch } from '@hirosystems/api-toolkit';
import { ENV } from '../env';
import { ParsedNakamotoBlock, ParsedStackerDbChunk } from './msg-parsing';
import { SignerMessagesEventPayload } from '../pg/types';
import { ThreadedParser } from './threaded-parser';
import { SERVER_VERSION } from '@hirosystems/api-toolkit';
import { EventEmitter } from 'node:events';
import { Message, MessagePath, StacksMessageStream } from '@stacks/node-publisher-client';

export class EventStreamHandler {
  db: PgStore;
  logger = defaultLogger.child({ name: 'EventStreamHandler' });
  eventStream: StacksMessageStream;
  threadedParser: ThreadedParser;

  readonly events = new EventEmitter<{
    processedMessage: [{ msgId: string }];
  }>();

  constructor(opts: { db: PgStore }) {
    this.db = opts.db;
    const appName = `signer-metrics-api ${SERVER_VERSION.tag} (${SERVER_VERSION.branch}:${SERVER_VERSION.commit})`;
    this.eventStream = new StacksMessageStream({
      appName,
      redisUrl: ENV.REDIS_URL,
      redisStreamPrefix: ENV.REDIS_STREAM_KEY_PREFIX,
      options: {
        selectedMessagePaths: [MessagePath.NewBlock, MessagePath.StackerDbChunks],
      },
    });
    this.threadedParser = new ThreadedParser();
  }

  async start() {
    await this.eventStream.connect({ waitForReady: true });
    this.eventStream.start(
      async () => {
        const chainTip = await this.db.getChainTip(this.db.sql);
        return {
          blockHeight: chainTip.block_height,
          indexBlockHash: chainTip.index_block_hash,
        };
      },
      async (messageId, timestamp, message) => {
        return this.handleMsg(messageId, timestamp, message);
      }
    );
  }

  async handleMsg(messageId: string, timestamp: string, message: Message) {
    this.logger.info(`${message.path}: received Stacks stream event, msgId: ${messageId}`);
    switch (message.path) {
      case MessagePath.NewBlock: {
        const blockMsg = message.payload;
        if (blockMsg.cycle_number && blockMsg.reward_set) {
          await this.db.ingestion.applyRewardSet(
            this.db.sql,
            blockMsg.cycle_number,
            blockMsg.reward_set
          );
        }
        if ('signer_signature_hash' in blockMsg) {
          const parsed = await this.threadedParser.parseNakamotoBlock(blockMsg);
          await this.handleNakamotoBlockMsg(parsed);
        } else {
          // ignore pre-Nakamoto blocks
        }
        break;
      }

      case MessagePath.StackerDbChunks: {
        const parsed = await this.threadedParser.parseStackerDbChunk(message.payload);
        await this.handleStackerDbMsg(parseInt(timestamp), parsed);
        break;
      }

      default:
        this.logger.warn(`Unhandled stacks stream event: ${message.path}`);
        break;
    }
    this.events.emit('processedMessage', { msgId: messageId });
  }

  async stop(): Promise<void> {
    await this.eventStream.stop();
    await this.threadedParser.close();
  }

  async handleStackerDbMsg(
    timestamp: number,
    stackerDbChunks: ParsedStackerDbChunk[]
  ): Promise<void> {
    const time = stopwatch();
    const appliedSignerMessageResults: SignerMessagesEventPayload = [];
    await this.db.ingestion.sqlWriteTransaction(async sql => {
      for (const chunk of stackerDbChunks) {
        const result = await this.db.ingestion.applyStackerDbChunk(sql, timestamp, chunk);
        appliedSignerMessageResults.push(...result);
      }
    });
    this.logger.info(`Apply StackerDB chunks finished in ${time.getElapsedSeconds()}s`);

    // After the sql transaction is complete, emit events for the applied signer messages.
    // Use setTimeout to break out of the call stack so caller is not blocked by event listeners.
    if (appliedSignerMessageResults.length > 0) {
      setTimeout(() => {
        this.db.ingestion.events.emit('signerMessages', appliedSignerMessageResults);
      });
    }
  }

  async handleNakamotoBlockMsg(block: ParsedNakamotoBlock): Promise<void> {
    const time = stopwatch();
    await this.db.sqlWriteTransaction(async sql => {
      const { block_height: lastIngestedBlockHeight } = await this.db.getChainTip(sql);
      if (block.blockHeight <= lastIngestedBlockHeight) {
        this.logger.info(`Skipping previously ingested block ${block.blockHeight}`);
        return;
      }
      this.logger.info(`Apply block ${block.blockHeight}`);
      await this.db.ingestion.applyBlock(sql, block);
      await this.db.ingestion.updateChainTip(sql, {
        blockHeight: block.blockHeight,
        indexBlockHash: block.indexBlockHash,
      });
    });
    this.logger.info(`Apply block ${block.blockHeight} finished in ${time.getElapsedSeconds()}s`);
  }
}
