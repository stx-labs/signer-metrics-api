import * as WorkerThreads from 'node:worker_threads';
import {
  ParsedNakamotoBlock,
  ParsedStackerDbChunk,
  parseNakamotoBlockMsg,
  parseStackerDbChunk,
} from './msg-parsing';
import { NewBlockMessage, StackerDbChunksMessage } from '@stacks/node-publisher-client';

export const workerFile = __filename;

export enum ThreadedParserMsgType {
  NakamotoBlock = 'NakamotoBlock',
  StackerDbChunk = 'StackerDbChunk',
}

interface ThreadMsg {
  type: ThreadedParserMsgType;
  msgId: number;
}

export interface NakamotoBlockMsgRequest extends ThreadMsg {
  type: ThreadedParserMsgType.NakamotoBlock;
  msgId: number;
  block: NewBlockMessage;
}

export interface NakamotoBlockMsgReply extends ThreadMsg {
  type: ThreadedParserMsgType.NakamotoBlock;
  msgId: number;
  block: ParsedNakamotoBlock;
}

export interface StackerDbChunkMsgRequest extends ThreadMsg {
  type: ThreadedParserMsgType.StackerDbChunk;
  msgId: number;
  chunk: StackerDbChunksMessage;
}

export interface StackerDbChunkMsgReply extends ThreadMsg {
  type: ThreadedParserMsgType.StackerDbChunk;
  msgId: number;
  chunk: ParsedStackerDbChunk[];
}

export type ThreadedParserMsgRequest = NakamotoBlockMsgRequest | StackerDbChunkMsgRequest;
export type ThreadedParserMsgReply = NakamotoBlockMsgReply | StackerDbChunkMsgReply;

if (!WorkerThreads.isMainThread) {
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const mainThreadPort = WorkerThreads.parentPort!;
  mainThreadPort.on('messageerror', err => {
    console.error(`Worker thread message error`, err);
  });
  mainThreadPort.on('message', (msg: ThreadedParserMsgRequest) => {
    try {
      handleWorkerMsg(msg);
    } catch (err) {
      console.error(`Failed to parse message: ${JSON.stringify(msg)}`);
      console.error(`Error handling message from main thread`, err);
    }
  });
}

function handleWorkerMsg(msg: ThreadedParserMsgRequest) {
  let reply: ThreadedParserMsgReply;
  switch (msg.type) {
    case ThreadedParserMsgType.NakamotoBlock: {
      reply = {
        type: ThreadedParserMsgType.NakamotoBlock,
        msgId: msg.msgId,
        block: parseNakamotoBlockMsg(msg.block),
      } satisfies NakamotoBlockMsgReply;
      break;
    }
    case ThreadedParserMsgType.StackerDbChunk: {
      reply = {
        type: ThreadedParserMsgType.StackerDbChunk,
        msgId: msg.msgId,
        chunk: parseStackerDbChunk(msg.chunk),
      } satisfies StackerDbChunkMsgReply;
      break;
    }
    default: {
      const _exhaustiveCheck: never = msg;
      throw new Error(`Unhandled message type: ${msg}`);
    }
  }
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  const mainThreadPort = WorkerThreads.parentPort!;
  mainThreadPort.postMessage(reply);
}
