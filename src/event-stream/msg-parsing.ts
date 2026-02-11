import * as crypto from 'node:crypto';
import * as secp from '../vendored/@noble/secp256k1';
import { BufferCursor } from './buffer-cursor';
import { BufferWriter } from './buffer-writer';
import { BytesReader, deserializeTransaction } from '@stacks/transactions';
import {
  NewBlockMessage,
  StackerDbChunksMessage,
  StackerDbChunksModifiedSlot,
} from '@stacks/node-publisher-client';

export interface ParsedNakamotoBlock {
  blockHeight: number;
  blockHash: string;
  burnBlockHeight: number;
  burnBlockHash: string;
  indexBlockHash: string;
  tenureHeight: number | null;
  blockTime: number;
  signerSignatures: string[];
  signerBitvec: string | null;
  signerPubKeys: string[];
  rewardSet: ParsedRewardSet | null;
  cycleNumber: number | null;
}

export interface ParsedRewardSet {
  pox_ustx_threshold: string; // "666720000000000"
  rewarded_addresses: string[]; // burnchain (btc) addresses
  signers?: {
    signing_key: string; // "03a80704b1eb07b4d526f069d6ac592bb9b8216bcf1734fa40badd8f9867b4c79e",
    weight: number; // 1,
    stacked_amt: string; // "3000225000000000"
  }[];
  start_cycle_state: {
    missed_reward_slots: [];
  };
}

export function parseNakamotoBlockMsg(block: NewBlockMessage): ParsedNakamotoBlock {
  const signerPubkeys = recoverBlockSignerPubkeys(block);
  const blockData: ParsedNakamotoBlock = {
    blockHeight: block.block_height,
    blockHash: block.block_hash,
    burnBlockHeight: block.burn_block_height,
    burnBlockHash: block.burn_block_hash,
    indexBlockHash: block.index_block_hash,
    tenureHeight: block.tenure_height ?? null,
    blockTime: block.block_time ?? 0,
    signerSignatures: block.signer_signature ?? [],
    signerBitvec: block.signer_bitvec ?? null,
    rewardSet: block.reward_set ?? null,
    cycleNumber: block.cycle_number ?? null,
    signerPubKeys: signerPubkeys,
  };
  return blockData;
}

interface ChunkMetadata {
  pubkey: string;
  contract: string;
  sig: string;
}

export interface BlockProposalChunkType extends ChunkMetadata {
  messageType: 'BlockProposal';
  blockProposal: ReturnType<typeof parseBlockProposal>;
}

export interface BlockResponseChunkType extends ChunkMetadata {
  messageType: 'BlockResponse';
  blockResponse: ReturnType<typeof parseBlockResponse>;
}

export interface BlockPushedChunkType extends ChunkMetadata {
  messageType: 'BlockPushed';
  blockPushed: ReturnType<typeof parseBlockPushed>;
}

export interface MockProposalChunkType extends ChunkMetadata {
  messageType: 'MockProposal';
  mockProposal: ReturnType<typeof parseMockProposal>;
}

export interface MockSignatureChunkType extends ChunkMetadata {
  messageType: 'MockSignature';
  mockSignature: ReturnType<typeof parseMockSignature>;
}

export interface MockBlockChunkType extends ChunkMetadata {
  messageType: 'MockBlock';
  mockBlock: ReturnType<typeof parseMockBlock>;
}

// https://github.com/stacks-network/stacks-core/blob/9d4cc3acd2c07d103b16750c1f3bdd6bf99a5232/libsigner/src/v0/messages.rs#L551
export interface StateMachineUpdate extends ChunkMetadata {
  messageType: 'StateMachineUpdate';

  // burn_block: ConsensusHash,
  // burn_block_height: u64,
  // current_miner_pkh: Hash160,
  // parent_tenure_id: ConsensusHash,
  // parent_tenure_last_block: StacksBlockId,
  // parent_tenure_last_block_height: u64,
  // active_signer_protocol_version: u64,
  // local_supported_signer_protocol_version: u64,
}

// https://github.com/stacks-network/stacks-core/blob/develop/libsigner/src/v0/messages.rs#L191
export interface BlockPreCommitChunkType extends ChunkMetadata {
  messageType: 'BlockPreCommit';
}

export type ParsedStackerDbChunk =
  | BlockProposalChunkType
  | BlockResponseChunkType
  | BlockPushedChunkType
  | MockProposalChunkType
  | MockSignatureChunkType
  | MockBlockChunkType
  | StateMachineUpdate
  | BlockPreCommitChunkType;

export function parseStackerDbChunk(chunk: StackerDbChunksMessage): ParsedStackerDbChunk[] {
  return chunk.modified_slots.flatMap(msg => {
    return {
      contract: chunk.contract_id.name,
      pubkey: recoverChunkSlotPubkey(msg).pubkey,
      sig: msg.sig,
      ...parseSignerMessage(Buffer.from(msg.data, 'hex')),
    };
  });
}

function recoverBlockSignerPubkeys(block: NewBlockMessage): string[] {
  const sigHash = Buffer.from(block.signer_signature_hash.replace(/^0x/, ''), 'hex');

  return (
    block.signer_signature?.map(signerSig => {
      const signerSigBuff = Buffer.from(signerSig.replace(/^0x/, ''), 'hex');
      const recid = signerSigBuff[0]; // recovery ID (first byte of the signature)
      const sig = signerSigBuff.subarray(1); // actual signature (remaining 64 bytes)

      const pubkey = secp.Signature.fromCompact(sig)
        .addRecoveryBit(recid)
        .recoverPublicKey(sigHash);

      return pubkey.toHex();
    }) ?? []
  );
}

enum SignerMessageTypePrefix {
  BlockProposal = 0,
  BlockResponse = 1,
  BlockPushed = 2,
  MockProposal = 3,
  MockSignature = 4,
  MockBlock = 5,
  StateMachineUpdate = 6,
  BlockPreCommit = 7,
}

// https://github.com/stacks-network/stacks-core/blob/cd702e7dfba71456e4983cf530d5b174e34507dc/libsigner/src/v0/messages.rs#L206
function parseSignerMessage(msg: Buffer) {
  const cursor = new BufferCursor(msg);
  const messageType = cursor.readU8Enum(SignerMessageTypePrefix);

  switch (messageType) {
    case SignerMessageTypePrefix.BlockProposal:
      return {
        messageType: 'BlockProposal',
        blockProposal: parseBlockProposal(cursor),
      } as const;
    case SignerMessageTypePrefix.BlockResponse:
      return {
        messageType: 'BlockResponse',
        blockResponse: parseBlockResponse(cursor),
      } as const;
    case SignerMessageTypePrefix.BlockPushed:
      return {
        messageType: 'BlockPushed',
        blockPushed: parseBlockPushed(cursor),
      } as const;
    case SignerMessageTypePrefix.MockProposal:
      return {
        messageType: 'MockProposal',
        mockProposal: parseMockProposal(cursor),
      } as const;
    case SignerMessageTypePrefix.MockSignature:
      return {
        messageType: 'MockSignature',
        mockSignature: parseMockSignature(cursor),
      } as const;
    case SignerMessageTypePrefix.MockBlock:
      return {
        messageType: 'MockBlock',
        mockBlock: parseMockBlock(cursor),
      } as const;
    case SignerMessageTypePrefix.StateMachineUpdate:
      return {
        messageType: 'StateMachineUpdate',
      } as const;
    case SignerMessageTypePrefix.BlockPreCommit:
      return {
        messageType: 'BlockPreCommit',
      } as const;
    default:
      throw new Error(`Unknown message type prefix: ${messageType}`);
  }
}

/** Convert a u32 integer into a 4 byte big-endian buffer */
function toU32BeBytes(num: number): Buffer {
  const buf = Buffer.alloc(4);
  buf.writeUInt32BE(num, 0);
  return buf;
}

/** Get the digest to sign that authenticates this chunk data and metadata */
function authDigest(slot: StackerDbChunksModifiedSlot): Buffer {
  const hasher = crypto.createHash('sha512-256');
  hasher.update(toU32BeBytes(slot.slot_id));
  hasher.update(toU32BeBytes(slot.slot_version));

  // Calculate the hash of the chunk bytes. This is the SHA512/256 hash of the data
  const dataHash = crypto.hash('sha512-256', Buffer.from(slot.data, 'hex'), 'buffer');
  hasher.update(dataHash);

  return hasher.digest();
}

function recoverChunkSlotPubkey(slot: StackerDbChunksModifiedSlot) {
  const digest = authDigest(slot);
  const sigBuff = Buffer.from(slot.sig, 'hex');

  const recid = sigBuff[0]; // recovery ID (first byte of the signature)
  const sig = sigBuff.subarray(1); // actual signature (remaining 64 bytes)

  const pubkey = secp.Signature.fromCompact(sig).addRecoveryBit(recid).recoverPublicKey(digest);

  return {
    pubkey: pubkey.toHex(),
    // pubkeyHash: crypto.hash('ripemd160', pubkey.toRawBytes(), 'hex'),
  };
}

// https://github.com/stacks-network/stacks-core/blob/cd702e7dfba71456e4983cf530d5b174e34507dc/libsigner/src/events.rs#L74
function parseBlockProposal(cursor: BufferCursor) {
  const block = parseNakamotoBlock(cursor);
  const burnHeight = cursor.readU64BE();
  const rewardCycle = cursor.readU64BE();
  return { block, burnHeight, rewardCycle };
}

// https://github.com/stacks-network/stacks-core/blob/30acb47f0334853def757b877773ae3ec45c6ba5/stackslib/src/chainstate/stacks/transaction.rs#L682-L692
function parseStacksTransaction(cursor: BufferCursor) {
  const bytesReader = new BytesReader(cursor.buffer);
  const stacksTransaction = deserializeTransaction(bytesReader);
  cursor.buffer = cursor.buffer.subarray(bytesReader.consumed);
  return stacksTransaction;
}

// https://github.com/stacks-network/stacks-core/blob/30acb47f0334853def757b877773ae3ec45c6ba5/stackslib/src/chainstate/nakamoto/mod.rs#L4547-L4550
function parseNakamotoBlock(cursor: BufferCursor) {
  const header = parseNakamotoBlockHeader(cursor);
  const blockHash = getNakamotoBlockHash(header);
  const indexBlockHash = getIndexBlockHash(blockHash, header.consensusHash);
  const tx = cursor.readArray(parseStacksTransaction);
  return { blockHash, indexBlockHash, header, tx };
}

// https://github.com/stacks-network/stacks-core/blob/a2dcd4c3ffdb625a6478bb2c0b23836bc9c72f9f/stacks-common/src/types/chainstate.rs#L268-L279
function getIndexBlockHash(blockHash: string, consensusHash: string): string {
  const hasher = crypto.createHash('sha512-256');
  hasher.update(Buffer.from(blockHash, 'hex'));
  hasher.update(Buffer.from(consensusHash, 'hex'));
  return hasher.digest('hex');
}

// https://github.com/stacks-network/stacks-core/blob/30acb47f0334853def757b877773ae3ec45c6ba5/stackslib/src/chainstate/nakamoto/mod.rs#L696-L711
function parseNakamotoBlockHeader(cursor: BufferCursor) {
  const version = cursor.readU8();
  const chainLength = cursor.readU64BE();
  const burnSpent = cursor.readU64BE();
  const consensusHash = cursor.readBytes(20).toString('hex');
  const parentBlockId = cursor.readBytes(32).toString('hex');
  const txMerkleRoot = cursor.readBytes(32).toString('hex');
  const stateIndexRoot = cursor.readBytes(32).toString('hex');
  const timestamp = cursor.readU64BE();
  const minerSignature = cursor.readBytes(65).toString('hex');
  const signerSignature = cursor.readArray(c => c.readBytes(65).toString('hex'));
  const poxTreatment = cursor.readBitVec();

  return {
    version,
    chainLength,
    burnSpent,
    consensusHash,
    parentBlockId,
    txMerkleRoot,
    stateIndexRoot,
    timestamp,
    minerSignature,
    signerSignature,
    poxTreatment,
  };
}

// https://github.com/stacks-network/stacks-core/blob/a2dcd4c3ffdb625a6478bb2c0b23836bc9c72f9f/stackslib/src/chainstate/nakamoto/mod.rs#L764-L795
function getNakamotoBlockHash(blockHeader: ReturnType<typeof parseNakamotoBlockHeader>): string {
  const blockHeaderBytes = new BufferWriter();
  blockHeaderBytes.writeU8(blockHeader.version);
  blockHeaderBytes.writeU64BE(blockHeader.chainLength);
  blockHeaderBytes.writeU64BE(blockHeader.burnSpent);
  blockHeaderBytes.writeBytes(Buffer.from(blockHeader.consensusHash, 'hex'));
  blockHeaderBytes.writeBytes(Buffer.from(blockHeader.parentBlockId, 'hex'));
  blockHeaderBytes.writeBytes(Buffer.from(blockHeader.txMerkleRoot, 'hex'));
  blockHeaderBytes.writeBytes(Buffer.from(blockHeader.stateIndexRoot, 'hex'));
  blockHeaderBytes.writeU64BE(blockHeader.timestamp);
  blockHeaderBytes.writeBytes(Buffer.from(blockHeader.minerSignature, 'hex'));
  blockHeaderBytes.writeBitVec(blockHeader.poxTreatment);
  const blockHash = crypto.hash('sha512-256', blockHeaderBytes.buffer, 'hex');
  return blockHash;
}

enum BlockResponseTypePrefix {
  // An accepted block response
  Accepted = 0,
  // A rejected block response
  Rejected = 1,
}

// https://github.com/stacks-network/stacks-core/blob/cd702e7dfba71456e4983cf530d5b174e34507dc/libsigner/src/v0/messages.rs#L650
function parseBlockResponse(cursor: BufferCursor) {
  const typePrefix = cursor.readU8Enum(BlockResponseTypePrefix);
  switch (typePrefix) {
    case BlockResponseTypePrefix.Accepted: {
      const signerSignatureHash = cursor.readBytes(32).toString('hex');
      const signature = cursor.readBytes(65).toString('hex');
      const metadata = parseSignerMessageMetadata(cursor);
      return { type: 'accepted', signerSignatureHash, signature, metadata } as const;
    }
    case BlockResponseTypePrefix.Rejected: {
      const reason = cursor.readVecString();
      const reasonCode = parseBlockResponseRejectCode(cursor);
      const signerSignatureHash = cursor.readBytes(32).toString('hex');
      const chainId = cursor.readU32BE();
      const signature = cursor.readBytes(65).toString('hex');
      const metadata = parseSignerMessageMetadata(cursor);
      return {
        type: 'rejected',
        reason,
        reasonCode,
        signerSignatureHash,
        chainId,
        signature,
        metadata,
      } as const;
    }
    default:
      throw new Error(`Unknown block response type prefix: ${typePrefix}`);
  }
}

function parseSignerMessageMetadata(cursor: BufferCursor) {
  if (cursor.buffer.length === 0) {
    return null;
  }
  return { server_version: cursor.readVecString() };
}

// https://github.com/stacks-network/stacks-core/blob/e45867c1781a7c45541e765cd42b6a084996fec8/libsigner/src/v0/messages.rs#L934
const RejectCodeTypePrefix = {
  // The block was rejected due to validation issues
  ValidationFailed: 0,
  // The block was rejected due to connectivity issues with the signer
  ConnectivityIssues: 1,
  // The block was rejected in a prior round
  RejectedInPriorRound: 2,
  // The block was rejected due to no sortition view
  NoSortitionView: 3,
  // The block was rejected due to a mismatch with expected sortition view
  SortitionViewMismatch: 4,
  // The block was rejected due to a testing directive
  TestingDirective: 5,
};

enum ValidateRejectCode {
  /// The block was rejected due to validation issues
  ValidationFailed = 0,
  /// The block was rejected due to connectivity issues with the signer
  ConnectivityIssues = 1,
  /// The block was rejected in a prior round
  RejectedInPriorRound = 2,
  /// The block was rejected due to no sortition view
  NoSortitionView = 3,
  /// The block was rejected due to a mismatch with expected sortition view
  SortitionViewMismatch = 4,
  /// The block was rejected due to a testing directive
  TestingDirective = 5,
  /// The block attempted to reorg the previous tenure but was not allowed
  ReorgNotAllowed = 6,
  /// The bitvec field does not match what is expected
  InvalidBitvec = 7,
  /// The miner's pubkey hash does not match the winning pubkey hash
  PubkeyHashMismatch = 8,
  /// The miner has been marked as invalid
  InvalidMiner = 9,
  /// Miner is last sortition winner, when the current sortition winner is
  /// still valid
  NotLatestSortitionWinner = 10,
  /// The block does not confirm the expected parent block
  InvalidParentBlock = 11,
  /// The block contains a block found tenure change, but we've already seen
  /// a block found
  DuplicateBlockFound = 12,
  /// The block attempted a tenure extend but the burn view has not changed
  /// and not enough time has passed for a time-based tenure extend
  InvalidTenureExtend = 13,
  /// Unknown reject code, for forward compatibility
  Unknown = 254,
  /// The block was approved, no rejection details needed
  NotRejected = 255,
}

// https://github.com/stacks-network/stacks-core/blob/cd702e7dfba71456e4983cf530d5b174e34507dc/libsigner/src/v0/messages.rs#L812
function parseBlockResponseRejectCode(cursor: BufferCursor) {
  const rejectCode = cursor.readU8Enum(RejectCodeTypePrefix);
  switch (rejectCode) {
    case RejectCodeTypePrefix.ValidationFailed: {
      const validateRejectCode = cursor.readU8Enum(ValidateRejectCode);
      return {
        rejectCode: getEnumName(RejectCodeTypePrefix, rejectCode),
        validateRejectCode: getEnumName(ValidateRejectCode, validateRejectCode),
      } as const;
    }
    case RejectCodeTypePrefix.ConnectivityIssues:
    case RejectCodeTypePrefix.RejectedInPriorRound:
    case RejectCodeTypePrefix.NoSortitionView:
    case RejectCodeTypePrefix.SortitionViewMismatch:
    case RejectCodeTypePrefix.TestingDirective:
      return {
        rejectCode: getEnumName(RejectCodeTypePrefix, rejectCode),
      } as const;
    default:
      throw new Error(`Unknown reject code type prefix: ${rejectCode}`);
  }
}

function getEnumName<T extends Record<string | number, string | number>>(
  enumObj: T,
  value: T[keyof T]
): Extract<keyof T, string> {
  const key = Object.keys(enumObj).find(key => enumObj[key] === value) as
    | Extract<keyof T, string>
    | undefined;
  if (!key) {
    throw new Error(`Value ${value} is not a valid enum value.`);
  }
  return key;
}
function parseBlockPushed(cursor: BufferCursor) {
  const block = parseNakamotoBlock(cursor);
  return block;
}
function parseMockProposal(cursor: BufferCursor) {
  const peerInfo = parseMockPeerInfo(cursor);
  const signature = cursor.readBytes(65).toString('hex');
  return { peerInfo, signature };
}

// https://github.com/stacks-network/stacks-core/blob/09d920cd1926422a8e3fb76fe4e1b1ef649546b4/libsigner/src/v0/messages.rs#L283
function parseMockPeerInfo(cursor: BufferCursor) {
  const burnBlockHeight = cursor.readU64BE();
  const stacksTipConsensusHash = cursor.readBytes(20).toString('hex');
  const stacksTip = cursor.readBytes(32).toString('hex');
  const stacksTipHeight = cursor.readU64BE();
  const serverVersion = cursor.readU8LengthPrefixedString();
  const poxConsensusHash = cursor.readBytes(20).toString('hex');
  const networkId = cursor.readU32BE();
  const indexBlockHash = getIndexBlockHash(stacksTip, stacksTipConsensusHash);
  return {
    burnBlockHeight,
    stacksTipConsensusHash,
    stacksTip,
    stacksTipHeight,
    serverVersion,
    poxConsensusHash,
    networkId,
    indexBlockHash,
  };
}

function parseMockSignature(cursor: BufferCursor) {
  const signature = cursor.readBytes(65).toString('hex');
  const mockProposal = parseMockProposal(cursor);
  const metadata = parseSignerMessageMetadata(cursor);
  return {
    signature,
    mockProposal,
    metadata,
  };
}

function parseMockBlock(cursor: BufferCursor) {
  const mockProposal = parseMockProposal(cursor);
  const mockSignatures = cursor.readArray(parseMockSignature);
  return { mockProposal, mockSignatures };
}
