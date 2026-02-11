import { PgNumeric, PgBytea } from '@hirosystems/api-toolkit';

export type DbBlock = {
  block_height: number;
  tenure_height: number;
  block_hash: PgBytea;
  block_time: string;
  index_block_hash: PgBytea;
  burn_block_height: number;
  burn_block_hash: PgBytea;
  is_nakamoto_block: boolean;
};

export type DbChainTip = {
  block_height: number;
  index_block_hash: string;
};

export type DbBlockSignerSignature = {
  block_height: number;
  signer_key: PgBytea;
  signer_signature: PgBytea;
};

export type DbRewardSetSigner = {
  cycle_number: number;
  signer_key: PgBytea;
  signer_weight: number;
  signer_stacked_amount: PgNumeric;
  slot_index: number;
};

export type DbBlockResponse = {
  received_at: string | Date;
  signer_key: PgBytea;
  accepted: boolean;
  signer_sighash: PgBytea;
  metadata_server_version: string;
  signature: PgBytea | null;
  reason_string: string | null;
  reason_code: string | null;
  reject_code: string | null;
  chain_id: number | null;
};

export type DbBlockPush = {
  received_at: string | Date;
  miner_key: PgBytea;
  block_height: number;
  block_time: string | Date;
  block_hash: PgBytea;
  index_block_hash: PgBytea;
};

export type DbBlockProposal = {
  received_at: string | Date;
  miner_key: PgBytea;
  block_height: number;
  block_time: string | Date;
  block_hash: PgBytea;
  index_block_hash: PgBytea;
  burn_block_height: number;
  reward_cycle: number;
};

export type DbBlockProposalQueryResponse = {
  // block proposal data (from block_proposals):
  received_at: Date;
  block_height: number;
  block_hash: string;
  index_block_hash: string;
  burn_block_height: number;
  block_time: number;
  cycle_number: number;

  // proposal status (from blocks table, matched using block_hash and block_height):
  status: 'pending' | 'rejected' | 'accepted';

  // milliseconds taken between proposal and block_push (from block_pushes, matched using block_hash):
  push_time_ms: number | null;

  // cycle data (from reward_set_signers, matched using cycle_number AKA reward_cycle):
  total_signer_count: number;
  total_signer_weight: number;
  total_signer_stacked_amount: string;

  // aggregate signer response data (from block_responses, matched using block_hash AKA signer_sighash, where missing is detected by the absence of a block_response for a given signer_key from the reward_set_signers table):
  accepted_count: number;
  rejected_count: number;
  missing_count: number;
  accepted_weight: number;
  rejected_weight: number;
  missing_weight: number;

  // signer responses (from block_responses, matched using block_hash AKA signer_sighash, using the signer_key from the reward_set_signers table for some of the fields):
  signer_data: {
    signer_key: string;
    slot_index: number;
    response: 'accepted' | 'rejected' | 'missing';
    weight: number;
    stacked_amount: string;

    version: string | null; // null for missing responses
    received_at: string | null; // null for missing responses

    // rejected fields (null for accepted and missing responses):
    reason_string: string | null;
    reason_code: string | null;
    reject_code: string | null;
  }[];
};

export type BlockProposalEventArgs = {
  receiptTimestamp: number;
  blockHash: string;
};
export type BlockResponseEventArgs = {
  receiptTimestamp: number;
  blockHash: string;
  signerKey: string;
};
export type BlockPushEventArgs = {
  receiptTimestamp: number;
  blockHash: string;
};

export type SignerMessagesEventPayload = (
  | {
      proposal: BlockProposalEventArgs;
    }
  | {
      response: BlockResponseEventArgs;
    }
  | {
      push: BlockPushEventArgs;
    }
)[];
