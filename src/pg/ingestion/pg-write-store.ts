import {
  BasePgStore,
  BasePgStoreModule,
  PgSqlClient,
  batchIterate,
  logger as defaultLogger,
} from '@hirosystems/api-toolkit';
import {
  DbBlock,
  DbBlockProposal,
  DbBlockPush,
  DbBlockResponse,
  DbBlockSignerSignature,
  DbRewardSetSigner,
  SignerMessagesEventPayload,
} from '../types';
import { normalizeHexString, unixTimeMillisecondsToISO, unixTimeSecondsToISO } from '../../helpers';
import { EventEmitter } from 'node:events';
import {
  BlockProposalChunkType,
  BlockPushedChunkType,
  BlockResponseChunkType,
  ParsedNakamotoBlock,
  ParsedRewardSet,
  ParsedStackerDbChunk,
} from '../../event-stream/msg-parsing';

export type DbWriteEvents = EventEmitter<{
  missingStackerSet: [{ cycleNumber: number }];
  signerMessages: [SignerMessagesEventPayload];
}>;

export class PgWriteStore extends BasePgStoreModule {
  readonly events: DbWriteEvents = new EventEmitter();
  readonly logger = defaultLogger.child({ module: 'PgWriteStore' });

  constructor(db: BasePgStore) {
    super(db);
  }

  async updateChainTip(
    sql: PgSqlClient,
    chainTip: { blockHeight: number; indexBlockHash: string }
  ): Promise<void> {
    await sql`
      UPDATE chain_tip SET
        block_height = ${chainTip.blockHeight},
        index_block_hash = ${chainTip.indexBlockHash}
    `;
  }

  async applyStackerDbChunk(
    sql: PgSqlClient,
    timestamp: number,
    chunk: ParsedStackerDbChunk
  ): Promise<SignerMessagesEventPayload> {
    const appliedResults: SignerMessagesEventPayload = [];
    switch (chunk.messageType) {
      case 'BlockProposal': {
        const res = await this.applyBlockProposal(sql, timestamp, chunk.pubkey, chunk);
        if (res.applied) {
          appliedResults.push({
            proposal: {
              receiptTimestamp: timestamp,
              blockHash: res.blockHash,
            },
          });
        }
        break;
      }
      case 'BlockResponse': {
        const res = await this.applyBlockResponse(sql, timestamp, chunk.pubkey, chunk);
        if (res.applied) {
          appliedResults.push({
            response: {
              receiptTimestamp: timestamp,
              blockHash: res.blockHash,
              signerKey: res.signerKey,
            },
          });
        }
        break;
      }
      case 'BlockPushed': {
        const res = await this.applyBlockPush(sql, timestamp, chunk.pubkey, chunk);
        if (res.applied) {
          appliedResults.push({
            push: {
              receiptTimestamp: timestamp,
              blockHash: res.blockHash,
            },
          });
        }
        break;
      }
      case 'MockProposal': {
        // ignore
        break;
      }
      case 'MockSignature': {
        // ignore
        break;
      }
      case 'MockBlock': {
        // ignore
        break;
      }
      case 'StateMachineUpdate': {
        // ignore
        break;
      }
      case 'BlockPreCommit': {
        // ignore
        break;
      }
      default: {
        this.logger.error(chunk, `Unknown StackerDB event type`);
        break;
      }
    }
    return appliedResults;
  }

  private async applyBlockProposal(
    sql: PgSqlClient,
    receivedAt: number,
    minerPubkey: string,
    messageData: BlockProposalChunkType
  ): Promise<{ applied: false } | { applied: true; blockHash: string }> {
    const blockHash = normalizeHexString(messageData.blockProposal.block.blockHash);
    const dbBlockProposal: DbBlockProposal = {
      received_at: unixTimeMillisecondsToISO(receivedAt),
      miner_key: normalizeHexString(minerPubkey),
      block_height: Number(messageData.blockProposal.block.header.chainLength),
      block_time: unixTimeSecondsToISO(Number(messageData.blockProposal.block.header.timestamp)),
      block_hash: blockHash,
      index_block_hash: normalizeHexString(messageData.blockProposal.block.indexBlockHash),
      reward_cycle: Number(messageData.blockProposal.rewardCycle),
      burn_block_height: Number(messageData.blockProposal.burnHeight),
    };
    const result = await sql`
      INSERT INTO block_proposals ${sql(dbBlockProposal)}
      ON CONFLICT ON CONSTRAINT block_proposals_block_hash_unique DO NOTHING
    `;
    if (result.count === 0) {
      this.logger.info(
        `Skipped inserting duplicate block proposal height=${dbBlockProposal.block_height}, hash=${dbBlockProposal.block_hash}`
      );
      return { applied: false };
    }
    this.logger.info(
      `Apply block_proposal height=${dbBlockProposal.block_height}, hash=${dbBlockProposal.block_hash}`
    );
    return { applied: true, blockHash };
  }

  async deleteBlockProposal(sql: PgSqlClient, blockHash: string): Promise<DbBlockProposal> {
    const result = await sql<DbBlockProposal[]>`
      DELETE FROM block_proposals WHERE block_hash = ${blockHash} RETURNING *
    `;
    if (result.length === 0) {
      throw new Error(`Block proposal not found for hash ${blockHash}`);
    }
    // copy the result to a new object to remove the id field
    const proposal = { ...result[0], id: undefined };
    delete proposal.id;
    return proposal;
  }

  async deleteBlockPush(sql: PgSqlClient, blockHash: string): Promise<DbBlockPush> {
    const result = await sql<DbBlockProposal[]>`
      DELETE FROM block_pushes WHERE block_hash = ${blockHash} RETURNING *
    `;
    if (result.length === 0) {
      throw new Error(`Block push not found for hash ${blockHash}`);
    }
    // copy the result to a new object to remove the id field
    const blockPush = { ...result[0], id: undefined };
    delete blockPush.id;
    return blockPush;
  }

  async deleteBlockResponses(sql: PgSqlClient, blockHash: string): Promise<DbBlockResponse[]> {
    const result = await sql<DbBlockResponse[]>`
      DELETE FROM block_responses WHERE signer_sighash = ${blockHash} RETURNING *
    `;
    // copy the results to a new object to remove the id field
    return result.map(r => {
      const response = { ...r, id: undefined };
      delete response.id;
      return response;
    });
  }

  private async applyBlockPush(
    sql: PgSqlClient,
    receivedAt: number,
    minerPubkey: string,
    messageData: BlockPushedChunkType
  ): Promise<{ applied: false } | { applied: true; blockHash: string }> {
    const blockHash = normalizeHexString(messageData.blockPushed.blockHash);
    const dbBlockPush: DbBlockPush = {
      received_at: unixTimeMillisecondsToISO(receivedAt),
      miner_key: normalizeHexString(minerPubkey),
      block_height: Number(messageData.blockPushed.header.chainLength),
      block_time: unixTimeSecondsToISO(Number(messageData.blockPushed.header.timestamp)),
      block_hash: blockHash,
      index_block_hash: normalizeHexString(messageData.blockPushed.indexBlockHash),
    };
    const result = await sql`
      INSERT INTO block_pushes ${sql(dbBlockPush)}
      ON CONFLICT ON CONSTRAINT block_pushes_block_hash_unique DO NOTHING
    `;

    if (result.count === 0) {
      this.logger.info(
        `Skipped inserting duplicate block push hash=${dbBlockPush.block_hash}, miner=${dbBlockPush.miner_key}`
      );
      return { applied: false };
    }
    this.logger.info(
      `Apply block_push hash=${dbBlockPush.block_hash}, miner=${dbBlockPush.miner_key}`
    );
    return { applied: true, blockHash };
  }

  private async applyBlockResponse(
    sql: PgSqlClient,
    receivedAt: number,
    signerPubkey: string,
    messageData: BlockResponseChunkType
  ): Promise<{ applied: false } | { applied: true; blockHash: string; signerKey: string }> {
    if (
      messageData.blockResponse.type !== 'accepted' &&
      messageData.blockResponse.type !== 'rejected'
    ) {
      this.logger.error(messageData, `Unexpected BlockResponse type`);
    }
    const received_at = unixTimeMillisecondsToISO(receivedAt);
    const metadata_server_version = messageData.blockResponse.metadata?.server_version ?? '';
    const accepted = messageData.blockResponse.type === 'accepted';

    const blockHash = normalizeHexString(messageData.blockResponse.signerSignatureHash);
    const signerKey = normalizeHexString(signerPubkey);
    const dbBlockResponse: DbBlockResponse = {
      received_at,
      signer_key: signerKey,
      accepted,
      signer_sighash: blockHash,
      metadata_server_version,
      signature: normalizeHexString(messageData.blockResponse.signature),
      reason_string: messageData.blockResponse.reason ?? null,
      reason_code: messageData.blockResponse.reasonCode?.rejectCode ?? null,
      reject_code: messageData.blockResponse.reasonCode?.validateRejectCode ?? null,
      chain_id: messageData.blockResponse.chainId ?? null,
    };
    const result = await sql`
      INSERT INTO block_responses ${sql(dbBlockResponse)}
      ON CONFLICT ON CONSTRAINT block_responses_signer_key_sighash_unique DO NOTHING
    `;

    if (result.count === 0) {
      this.logger.info(
        `Skipped inserting duplicate block response signer=${dbBlockResponse.signer_key}, hash=${dbBlockResponse.signer_sighash}`
      );
      return { applied: false };
    }

    // Update the `reward_set_signers` table with the last response time, average response time,
    // metadata server version, and proposal acceptance/rejection counts for this signer.
    // The `proposals_missed_count` will be updated once a block is confirmed.
    await sql`
      WITH proposal_data AS (
        SELECT reward_cycle, EXTRACT(EPOCH FROM received_at) * 1000 AS received_at
        FROM block_proposals
        WHERE block_hash = ${blockHash}
      )
      UPDATE reward_set_signers
      SET last_response_time = ${received_at},
        last_response_metadata_server_version = ${metadata_server_version},
        proposals_accepted_count = proposals_accepted_count + ${accepted ? 1 : 0},
        proposals_rejected_count = proposals_rejected_count + ${accepted ? 0 : 1},
        average_response_time_ms = (
          (average_response_time_ms * (proposals_accepted_count + proposals_rejected_count) + (${receivedAt} - (SELECT received_at FROM proposal_data))) /
          (proposals_accepted_count + proposals_rejected_count + 1)
        )
      WHERE signer_key = ${dbBlockResponse.signer_key}
        AND cycle_number = (SELECT reward_cycle FROM proposal_data)
    `;

    this.logger.info(
      `Apply block_response signer=${dbBlockResponse.signer_key}, hash=${dbBlockResponse.signer_sighash}`
    );
    return { applied: true, blockHash, signerKey };
  }

  async applyBlock(sql: PgSqlClient, block: ParsedNakamotoBlock) {
    const dbBlock: DbBlock = {
      block_height: block.blockHeight,
      block_hash: normalizeHexString(block.blockHash),
      index_block_hash: normalizeHexString(block.indexBlockHash),
      burn_block_height: block.burnBlockHeight,
      burn_block_hash: normalizeHexString(block.burnBlockHash),
      tenure_height: block.tenureHeight ?? 0,
      block_time: unixTimeSecondsToISO(block.blockTime ?? 0),
      is_nakamoto_block: !!block.signerBitvec,
    };
    await this.insertBlock(sql, dbBlock);

    const dbSignerSignatures = block.signerSignatures.map((sig, i) => {
      const dbSig: DbBlockSignerSignature = {
        block_height: dbBlock.block_height,
        signer_key: normalizeHexString(block.signerPubKeys[i]),
        signer_signature: normalizeHexString(sig),
      };
      return dbSig;
    });
    if (dbSignerSignatures) {
      await this.insertBlockSignerSignatures(sql, dbSignerSignatures);
    }

    if ((block.rewardSet?.signers ?? []).length > 0 && !block.cycleNumber) {
      throw new Error(`Missing cycle_number for block ${block.blockHeight} reward set`);
    }

    if (block.rewardSet) {
      await this.applyRewardSet(sql, block.cycleNumber ?? 0, block.rewardSet);
    }
  }

  async applyRewardSet(sql: PgSqlClient, cycleNumber: number, rewardSet: ParsedRewardSet) {
    const dbRewardSetSigners = rewardSet.signers?.map((signer, index) => {
      const dbSigner: DbRewardSetSigner = {
        cycle_number: cycleNumber,
        signer_key: normalizeHexString(signer.signing_key),
        signer_weight: signer.weight,
        signer_stacked_amount: signer.stacked_amt,
        slot_index: index,
      };
      return dbSigner;
    });
    if (dbRewardSetSigners && dbRewardSetSigners.length > 0) {
      await this.insertRewardSetSigners(sql, dbRewardSetSigners);
    }
  }

  private async insertBlock(sql: PgSqlClient, dbBlock: DbBlock) {
    // Skip pre-nakamoto blocks
    if (!dbBlock.is_nakamoto_block) {
      this.logger.info(
        `Skipping apply for pre-nakamoto block ${dbBlock.block_height} ${dbBlock.block_hash}`
      );
    } else {
      // After the block is inserted, calculate the reward_cycle_number, then check if the
      // reward_set_signers table contains any rows for the calculated cycle_number. If there are
      // any signers that are not in the block_responses table for this block, update their
      // `proposals_missed_count`.
      // TODO: add unique constraint here
      const result = await sql<{ cycle_number: number | null; reward_set_exists: boolean }[]>`
        WITH inserted AS (
          INSERT INTO blocks ${sql(dbBlock)}
          RETURNING burn_block_height
        ),
        cycle_number AS (
          SELECT FLOOR((inserted.burn_block_height - ct.first_burnchain_block_height) / ct.reward_cycle_length) AS cycle_number
          FROM inserted, chain_tip AS ct
          LIMIT 1
        ),
        signer_miss_update AS (
          UPDATE reward_set_signers
          SET proposals_missed_count = proposals_missed_count + 1
          WHERE cycle_number = (SELECT cycle_number FROM cycle_number)
            AND signer_key NOT IN (
              SELECT signer_key FROM block_responses WHERE signer_sighash = ${dbBlock.block_hash}
            )
        )
        SELECT 
          cn.cycle_number,
          EXISTS (
            SELECT 1 
            FROM reward_set_signers 
            WHERE cycle_number = cn.cycle_number
            LIMIT 1
          ) AS reward_set_exists
        FROM cycle_number AS cn
      `;
      const { cycle_number, reward_set_exists } = result[0];
      if (cycle_number === null) {
        this.logger.warn(`Failed to calculate cycle number for block ${dbBlock.block_height}`);
      } else if (cycle_number !== null && !reward_set_exists) {
        this.logger.warn(
          `Missing reward set signers for cycle ${cycle_number} in block ${dbBlock.block_height}`
        );
        // Use setTimeout to ensure we break out of the current sql transaction within the async context
        setTimeout(() => this.events.emit('missingStackerSet', { cycleNumber: cycle_number }));
      }
      this.logger.info(`Apply block ${dbBlock.block_height} ${dbBlock.block_hash}`);
    }
  }

  private async insertBlockSignerSignatures(
    sql: PgSqlClient,
    signerSigs: DbBlockSignerSignature[]
  ) {
    if (signerSigs.length === 0) {
      // nothing to insert
      return;
    }
    let insertCount = 0;
    for (const batch of batchIterate(signerSigs, 500)) {
      // TODO: add unique constraint here
      const result = await sql`
        INSERT INTO block_signer_signatures ${sql(batch)}
      `;
      insertCount += result.count;
    }
    if (insertCount === 0) {
      this.logger.info(
        `Skipped inserting duplicate block signer signatures for block ${signerSigs[0].block_height}`
      );
    } else {
      this.logger.info(
        `Apply block_signer_signatures, block=${signerSigs[0].block_height}, count=${insertCount}`
      );
    }
  }

  async insertRewardSetSigners(
    sql: PgSqlClient,
    rewardSetSigners: DbRewardSetSigner[]
  ): Promise<{ rowsDeleted: number; rowsInserted: number }> {
    if (rewardSetSigners.length === 0) {
      return { rowsDeleted: 0, rowsInserted: 0 };
    }

    const cycleNumber = rewardSetSigners[0].cycle_number;
    const deleteRows = await sql`
      DELETE FROM reward_set_signers
      WHERE cycle_number = ${cycleNumber}
    `;
    if (deleteRows.count > 0) {
      this.logger.warn(
        `Deleted existing reward set signers for cycle ${cycleNumber} before inserting new rows, deleted ${deleteRows.count} rows`
      );
    }

    let insertCount = 0;
    for (const batch of batchIterate(rewardSetSigners, 500)) {
      const result = await sql`
        INSERT INTO reward_set_signers ${sql(batch)}
        ON CONFLICT ON CONSTRAINT reward_set_signers_cycle_unique DO NOTHING
      `;
      insertCount += result.count;
    }
    if (insertCount === 0) {
      this.logger.info(
        `Skipped inserting duplicate reward set signers for cycle ${rewardSetSigners[0].cycle_number}`
      );
    } else {
      this.logger.info(
        `Apply reward_set_signers, cycle=${rewardSetSigners[0].cycle_number}, count=${insertCount}`
      );
      // Update the `signer_stacked_amount_percentage` and `signer_stacked_amount_rank` for each signer
      // in the `reward_set_signers` table.
      await sql`
        WITH total_stacked_amount AS (
          SELECT SUM(signer_stacked_amount) AS total
          FROM reward_set_signers
          WHERE cycle_number = ${cycleNumber}
        ),
        total_weight AS (
          SELECT SUM(signer_weight) AS total
          FROM reward_set_signers
          WHERE cycle_number = ${cycleNumber}
        ),
        signer_ranks AS (
          SELECT signer_key, RANK() OVER (ORDER BY signer_stacked_amount DESC, signer_key ASC) AS rank
          FROM reward_set_signers
          WHERE cycle_number = ${cycleNumber}
        )
        UPDATE reward_set_signers
        SET signer_stacked_amount_percentage = signer_stacked_amount::float8 / (SELECT total FROM total_stacked_amount)::float8,
          signer_stacked_amount_rank = (SELECT rank FROM signer_ranks WHERE signer_key = reward_set_signers.signer_key),
          signer_weight_percentage = signer_weight::float8 / (SELECT total FROM total_weight)::float8
        WHERE cycle_number = ${cycleNumber}
      `;
    }
    return { rowsDeleted: deleteRows.count, rowsInserted: insertCount };
  }

  async rollBackBlock(sql: PgSqlClient, blockHeight: number) {
    const res = await sql`
      DELETE FROM blocks WHERE block_height = ${blockHeight}
    `;
    this.logger.info(`Rollback block ${blockHeight}`);
    if (res.count !== 1) {
      this.logger.warn(
        `Unexpected number of rows deleted for block ${blockHeight}, ${res.count} rows`
      );
    }
  }

  async rollBackBlockSignerSignatures(sql: PgSqlClient, blockHeight: number) {
    const res = await sql`
      DELETE FROM block_signer_signatures WHERE block_height = ${blockHeight}
    `;
    this.logger.info(
      `Rollback block signer signatures for block ${blockHeight}, deleted ${res.count} rows`
    );
  }
}
