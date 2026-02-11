import { ENV } from '../env';
import {
  BasePgStore,
  PgConnectionArgs,
  PgSqlClient,
  connectPostgres,
  logger,
  runMigrations,
} from '@hirosystems/api-toolkit';
import * as path from 'path';
import { PgWriteStore } from './ingestion/pg-write-store';
import { BlockIdParam, normalizeHexString, sleep } from '../helpers';
import { Fragment } from 'postgres';
import { DbBlockProposalQueryResponse } from './types';
import { NotificationPgStore } from './notifications/pg-notifications';

export const MIGRATIONS_DIR = path.join(__dirname, '../../migrations');

/**
 * Connects and queries the Signer Metrics API's local postgres DB.
 */
export class PgStore extends BasePgStore {
  readonly ingestion: PgWriteStore;
  readonly notifications?: NotificationPgStore;

  static async connect(opts?: {
    skipMigrations?: boolean;
    /** If a PGSCHEMA is run `CREATE SCHEMA IF NOT EXISTS schema_name` */
    createSchema?: boolean;
    enableListenNotify?: boolean;
  }): Promise<PgStore> {
    const pgConfig: PgConnectionArgs = {
      host: ENV.PGHOST,
      port: ENV.PGPORT,
      user: ENV.PGUSER,
      password: ENV.PGPASSWORD,
      database: ENV.PGDATABASE,
      schema: ENV.PGSCHEMA,
    };
    const sql = await connectPostgres({
      usageName: 'signer-metrics-pg-store',
      connectionArgs: pgConfig,
      connectionConfig: {
        poolMax: ENV.PG_CONNECTION_POOL_MAX,
        idleTimeout: ENV.PG_IDLE_TIMEOUT,
        maxLifetime: ENV.PG_MAX_LIFETIME,
      },
    });

    if (pgConfig.schema && opts?.createSchema !== false) {
      await sql`CREATE SCHEMA IF NOT EXISTS ${sql(pgConfig.schema)}`;
    }
    if (opts?.skipMigrations !== true) {
      while (true) {
        try {
          logger.info('Running migrations, this may take a while...');
          await runMigrations(MIGRATIONS_DIR, 'up', pgConfig);
          break;
        } catch (error) {
          if (/Another migration is already running/i.test((error as Error).message)) {
            logger.warn('Another migration is already running, retrying...');
            await sleep(100);
            continue;
          }
          throw error;
        }
      }
    }
    return new PgStore(sql, opts?.enableListenNotify ?? false);
  }

  constructor(sql: PgSqlClient, enableListenNotify: boolean) {
    super(sql);
    this.ingestion = new PgWriteStore(this);
    if (enableListenNotify) {
      this.notifications = new NotificationPgStore(this, sql, this.ingestion.events);
    }
  }

  async getChainTip(
    sql: PgSqlClient
  ): Promise<{ block_height: number; index_block_hash: string } | null> {
    const result = await sql<{ block_height: number; index_block_hash: string }[]>`
      SELECT block_height, index_block_hash
      FROM blocks
      WHERE block_height = (SELECT block_height FROM chain_tip)
    `;
    return result[0] ?? null;
  }

  public async getLastIngestedRedisMsgId(): Promise<string> {
    const result = await this.sql<
      { last_redis_msg_id: string }[]
    >`SELECT last_redis_msg_id FROM chain_tip`;
    return result[0].last_redis_msg_id;
  }

  async getChainTipBlockHeight(): Promise<number> {
    const result = await this.sql<{ block_height: number }[]>`SELECT block_height FROM chain_tip`;
    return result[0].block_height;
  }

  async getPoxInfo() {
    const result = await this.sql<
      { first_burnchain_block_height: number | null; reward_cycle_length: number | null }[]
    >`
      SELECT first_burnchain_block_height, reward_cycle_length FROM chain_tip
    `;
    return result[0];
  }

  async updatePoxInfo(poxInfo: {
    first_burnchain_block_height: number;
    reward_cycle_length: number;
  }): Promise<{ rowUpdated: boolean }> {
    // Update the first_burnchain_block_height and reward_cycle_length columns in the chain_tip table only if
    // they differ from the existing values. Return true if the row was updated, false otherwise.
    // Should only update the row if the values are null (i.e. the first time the values are set).
    const updateResult = await this.sql`
      UPDATE chain_tip 
      SET 
        first_burnchain_block_height = ${poxInfo.first_burnchain_block_height},
        reward_cycle_length = ${poxInfo.reward_cycle_length}
      WHERE
        first_burnchain_block_height IS DISTINCT FROM ${poxInfo.first_burnchain_block_height} 
        OR reward_cycle_length IS DISTINCT FROM ${poxInfo.reward_cycle_length}
    `;
    return { rowUpdated: updateResult.count > 0 };
  }

  async getRecentBlockProposals({
    sql,
    limit,
    offset,
  }: {
    sql: PgSqlClient;
    limit: number;
    offset: number;
  }) {
    const result = await sql<DbBlockProposalQueryResponse[]>`
      SELECT 
        bp.received_at,
        bp.block_height,
        bp.block_hash,
        bp.index_block_hash,
        bp.burn_block_height,
        EXTRACT(EPOCH FROM bp.block_time)::integer AS block_time,
        bp.reward_cycle AS cycle_number,

        -- Proposal status
        CASE
          WHEN block_pushes.block_hash IS NOT NULL THEN 'accepted'
          WHEN bp.block_height > ct.block_height THEN 'pending'
          WHEN b.block_hash IS NULL THEN 'rejected'
          WHEN b.block_hash = bp.block_hash THEN 'accepted'
          ELSE 'rejected'
        END AS status,

        (EXTRACT(EPOCH FROM (block_pushes.received_at - bp.received_at)) * 1000)::integer AS push_time_ms,

        -- Aggregate cycle data from reward_set_signers
        COUNT(DISTINCT rss.signer_key)::integer AS total_signer_count,
        SUM(rss.signer_weight)::integer AS total_signer_weight,
        SUM(rss.signer_stacked_amount) AS total_signer_stacked_amount,

        -- Aggregate response data for accepted, rejected, and missing counts and weights
        COUNT(br.accepted) FILTER (WHERE br.accepted = TRUE)::integer AS accepted_count,
        COUNT(br.accepted) FILTER (WHERE br.accepted = FALSE)::integer AS rejected_count,
        COUNT(*) FILTER (WHERE br.id IS NULL)::integer AS missing_count,
        
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.accepted = TRUE), 0)::integer AS accepted_weight,
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.accepted = FALSE), 0)::integer AS rejected_weight,
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.id IS NULL), 0)::integer AS missing_weight,

        -- Array of signer response details
        COALESCE(
          JSON_AGG(
            json_build_object(
              'signer_key', '0x' || encode(rss.signer_key, 'hex'),
              'slot_index', rss.slot_index,
              'response', 
                CASE 
                  WHEN br.id IS NULL THEN 'missing'
                  WHEN br.accepted = TRUE THEN 'accepted'
                  ELSE 'rejected'
                END,
              'version', br.metadata_server_version,
              'weight', rss.signer_weight,
              'stacked_amount', rss.signer_stacked_amount::text,
              'received_at', to_char(br.received_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
              'reason_string', br.reason_string,
              'reason_code', br.reason_code,
              'reject_code', br.reject_code
            ) ORDER BY rss.slot_index
          ),
          '[]'::json
        ) AS signer_data

      FROM (
        SELECT * 
        FROM block_proposals
        ORDER BY received_at DESC
        LIMIT ${limit}
        OFFSET ${offset}
      ) AS bp

      -- Join with chain_tip to get the current block height
      CROSS JOIN chain_tip ct

      -- Join with blocks to check if there's a matching block for the same block_height and block_hash
      LEFT JOIN blocks b 
        ON b.block_height = bp.block_height

      LEFT JOIN block_pushes
        ON block_pushes.block_hash = bp.block_hash

      LEFT JOIN reward_set_signers rss 
        ON rss.cycle_number = bp.reward_cycle

      LEFT JOIN block_responses br 
        ON br.signer_sighash = bp.block_hash 
        AND br.signer_key = rss.signer_key

      GROUP BY 
        bp.id, 
        bp.received_at, 
        bp.block_height, 
        bp.block_hash, 
        bp.index_block_hash, 
        bp.burn_block_height, 
        bp.block_time, 
        bp.reward_cycle, 
        ct.block_height,
        b.block_hash,
        block_pushes.block_hash,
        block_pushes.received_at

      ORDER BY bp.received_at DESC
    `;
    return result;
  }

  async getBlockProposal({ sql, blockHash }: { sql: PgSqlClient; blockHash: string }) {
    const result = await sql<DbBlockProposalQueryResponse[]>`
      SELECT 
        bp.received_at,
        bp.block_height,
        bp.block_hash,
        bp.index_block_hash,
        bp.burn_block_height,
        EXTRACT(EPOCH FROM bp.block_time)::integer AS block_time,
        bp.reward_cycle AS cycle_number,

        -- Proposal status
        CASE
          WHEN block_pushes.block_hash IS NOT NULL THEN 'accepted'
          WHEN bp.block_height > ct.block_height THEN 'pending'
          WHEN b.block_hash IS NULL THEN 'rejected'
          WHEN b.block_hash = bp.block_hash THEN 'accepted'
          ELSE 'rejected'
        END AS status,

        (EXTRACT(EPOCH FROM (block_pushes.received_at - bp.received_at)) * 1000)::integer AS push_time_ms,

        -- Aggregate cycle data from reward_set_signers
        COUNT(DISTINCT rss.signer_key)::integer AS total_signer_count,
        SUM(rss.signer_weight)::integer AS total_signer_weight,
        SUM(rss.signer_stacked_amount) AS total_signer_stacked_amount,

        -- Aggregate response data for accepted, rejected, and missing counts and weights
        COUNT(br.accepted) FILTER (WHERE br.accepted = TRUE)::integer AS accepted_count,
        COUNT(br.accepted) FILTER (WHERE br.accepted = FALSE)::integer AS rejected_count,
        COUNT(*) FILTER (WHERE br.id IS NULL)::integer AS missing_count,
        
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.accepted = TRUE), 0)::integer AS accepted_weight,
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.accepted = FALSE), 0)::integer AS rejected_weight,
        COALESCE(SUM(rss.signer_weight) FILTER (WHERE br.id IS NULL), 0)::integer AS missing_weight,

        -- Array of signer response details
        COALESCE(
          JSON_AGG(
            json_build_object(
              'signer_key', '0x' || encode(rss.signer_key, 'hex'),
              'slot_index', rss.slot_index,
              'response', 
                CASE 
                  WHEN br.id IS NULL THEN 'missing'
                  WHEN br.accepted = TRUE THEN 'accepted'
                  ELSE 'rejected'
                END,
              'version', br.metadata_server_version,
              'weight', rss.signer_weight,
              'stacked_amount', rss.signer_stacked_amount::text,
              'received_at', to_char(br.received_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
              'reason_string', br.reason_string,
              'reason_code', br.reason_code,
              'reject_code', br.reject_code
            ) ORDER BY rss.slot_index
          ),
          '[]'::json
        ) AS signer_data

      FROM block_proposals bp

      -- Join with chain_tip to get the current block height
      CROSS JOIN chain_tip ct

      -- Join with blocks to check if there's a matching block for the same block_height and block_hash
      LEFT JOIN blocks b 
        ON b.block_height = bp.block_height

      LEFT JOIN block_pushes
        ON block_pushes.block_hash = bp.block_hash

      LEFT JOIN reward_set_signers rss 
        ON rss.cycle_number = bp.reward_cycle

      LEFT JOIN block_responses br 
        ON br.signer_sighash = bp.block_hash 
        AND br.signer_key = rss.signer_key

      -- Filter for a specific block proposal based on block_hash
      WHERE bp.block_hash = ${blockHash}

      GROUP BY 
        bp.id, 
        ct.block_height,
        b.block_hash,
        block_pushes.block_hash,
        block_pushes.received_at

      LIMIT 1
    `;
    return result;
  }

  async getSignerDataForRecentBlocks({
    sql,
    limit,
    offset,
  }: {
    sql: PgSqlClient;
    limit: number;
    offset: number;
  }) {
    // The `blocks` table (and its associated block_signer_signatures table) is the source of truth that is
    // never missing blocks and does not contain duplicate rows per block.
    //
    // Each block has a known set of signer_keys which can be determined by first looking up the block's
    // cycle_number from the `block_proposals` table matching on block_hash, then using cycle_number to look
    // up the set of signer_keys from the reward_set_signers table (matching cycle_number with reward_cycle).
    //
    // From the set of known signer_keys, we can then determine the following state for each signer_key:
    //  * accepted_mined: the signer_key is included in the block_signer_signatures table for the
    //    associatedblock, and there exists an associated block_responses entry for the signer_key
    //    and block, where accepted=true.
    //  * accepted_excluded: the signer_key is not included in block_signer_signatures, however, there
    //    exists an associated block_responses entry for the signer_key and block, where accepted=true.
    //  * rejected: the signer_key is not included in block_signer_signatures, however, there exists
    //    an associated block_responses entry for the signer_key and block, where accepted=false.
    //  * missing: the signer_key is not included in block_signer_signatures table and there is associated
    //    block_responses entry.
    //
    // For the accepted_mined, accepted_excluded, and rejected states we can determine response_time_ms (how
    // long it took each signer to submit a block_response) by comparing block_proposal.received_at to
    // block_response.received_at.
    //
    // Fetch the latest N blocks from the `blocks` table ordered by block_height DESC, and for each block
    // determine the following additional fields:
    //  * cycle_number: the cycle_number from the associated block_proposal
    //  * block_proposal_time_ms: The received_at from the associated block_proposal
    //  * total_signer_count: The count of known signer_keys (look them up from reward_set_signers).
    //  * signer_accepted_mined_count: The count of accepted_mined signer states
    //  * signer_accepted_excluded_count: The count of accepted_excluded signer states
    //  * signer_rejected_count: The count of rejected signer states
    //  * signer_missing_count: The count of missing signer states
    //  * average_response_time_ms: The average signer response_time_ms
    //  * accepted_mined_stacked_amount: the total signer_stacked_amount of each signer in the accepted_mined state
    //  * accepted_excluded_stacked_amount: the total signer_stacked_amount of each signer in the accepted_excluded state
    //  * rejected_stacked_amount: the total signer_stacked_amount of each signer in the rejected state
    //  * missing_stacked_amount: the total signer_stacked_amount of each signer in the missing state
    //  * accepted_mined_weight: the total signer_weight of each signer in the accepted_mined state
    //  * accepted_excluded_weight: the total signer_weight of each signer in the accepted_excluded state
    //  * rejected_weight: the total signer_weight of each signer in the rejected state
    //  * missing_weight: the total signer_weight of each signer in the missing state

    const result = await sql<
      {
        block_height: number;
        block_hash: string;
        index_block_hash: string;
        burn_block_height: number;
        tenure_height: number;
        block_time: number;
        cycle_number: number | null;
        block_proposal_time_ms: string | null;
        total_signer_count: number;
        signer_accepted_mined_count: number;
        signer_accepted_excluded_count: number;
        signer_rejected_count: number;
        signer_missing_count: number;
        average_response_time_ms: number;
        accepted_mined_stacked_amount: string;
        accepted_excluded_stacked_amount: string;
        rejected_stacked_amount: string;
        missing_stacked_amount: string;
        accepted_mined_weight: number;
        accepted_excluded_weight: number;
        rejected_weight: number;
        missing_weight: number;
        chain_tip_block_height: number;
      }[]
    >`
      WITH latest_blocks AS (
        SELECT * FROM blocks
        ORDER BY block_height DESC
        LIMIT ${limit}
        OFFSET ${offset}
      ),
      block_signers AS (
        SELECT
          lb.id AS block_id,
          lb.block_height,
          lb.block_time,
          lb.block_hash,
          lb.index_block_hash,
          lb.burn_block_height,
          bp.reward_cycle as cycle_number,
          bp.received_at AS block_proposal_time_ms,
          rs.signer_key,
          COALESCE(rs.signer_weight, 0) AS signer_weight,
          COALESCE(rs.signer_stacked_amount, 0) AS signer_stacked_amount,
          CASE
            WHEN bss.id IS NOT NULL THEN 'accepted_mined'
            WHEN bss.id IS NULL AND fbr.accepted = TRUE THEN 'accepted_excluded'
            WHEN bss.id IS NULL AND fbr.accepted = FALSE THEN 'rejected'
            WHEN bss.id IS NULL AND fbr.id IS NULL THEN 'missing'
          END AS signer_status,
          EXTRACT(EPOCH FROM (fbr.received_at - bp.received_at)) * 1000 AS response_time_ms
        FROM latest_blocks lb
        LEFT JOIN block_proposals bp ON lb.block_hash = bp.block_hash
        LEFT JOIN reward_set_signers rs ON bp.reward_cycle = rs.cycle_number
        LEFT JOIN block_signer_signatures bss ON lb.block_height = bss.block_height AND rs.signer_key = bss.signer_key
        LEFT JOIN block_responses fbr ON fbr.signer_key = rs.signer_key AND fbr.signer_sighash = lb.block_hash
      ),
      signer_state_aggregation AS (
        SELECT
          block_id,
          MAX(cycle_number) AS cycle_number,
          MAX(block_proposal_time_ms) AS block_proposal_time_ms,
          COUNT(signer_key) AS total_signer_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'accepted_mined' THEN 1 END), 0) AS signer_accepted_mined_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'accepted_excluded' THEN 1 END), 0) AS signer_accepted_excluded_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'rejected' THEN 1 END), 0) AS signer_rejected_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'missing' THEN 1 END), 0) AS signer_missing_count,
          COALESCE(AVG(response_time_ms) FILTER (WHERE signer_status IN ('accepted_mined', 'accepted_excluded', 'rejected')), 0) AS average_response_time_ms,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_mined' THEN signer_stacked_amount END), 0) AS accepted_mined_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_excluded' THEN signer_stacked_amount END), 0) AS accepted_excluded_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'rejected' THEN signer_stacked_amount END), 0) AS rejected_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'missing' THEN signer_stacked_amount END), 0) AS missing_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_mined' THEN signer_weight END), 0) AS accepted_mined_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_excluded' THEN signer_weight END), 0) AS accepted_excluded_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'rejected' THEN signer_weight END), 0) AS rejected_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'missing' THEN signer_weight END), 0) AS missing_weight
        FROM block_signers
        GROUP BY block_id
      )
      SELECT
          lb.block_height,
          lb.block_hash,
          lb.index_block_hash,
          lb.burn_block_height,
          lb.tenure_height,
          EXTRACT(EPOCH FROM lb.block_time)::integer AS block_time,
          bsa.cycle_number,
          (EXTRACT(EPOCH FROM bsa.block_proposal_time_ms) * 1000)::bigint AS block_proposal_time_ms,
          bsa.total_signer_count::integer,
          bsa.signer_accepted_mined_count::integer,
          bsa.signer_accepted_excluded_count::integer,
          bsa.signer_rejected_count::integer,
          bsa.signer_missing_count::integer,
          ROUND(bsa.average_response_time_ms, 3)::float8 as average_response_time_ms,
          bsa.accepted_mined_stacked_amount,
          bsa.accepted_excluded_stacked_amount,
          bsa.rejected_stacked_amount,
          bsa.missing_stacked_amount,
          bsa.accepted_mined_weight::integer,
          bsa.accepted_excluded_weight::integer,
          bsa.rejected_weight::integer,
          bsa.missing_weight::integer,
          ct.block_height AS chain_tip_block_height
      FROM latest_blocks lb
      JOIN signer_state_aggregation bsa ON lb.id = bsa.block_id
      CROSS JOIN chain_tip ct
      ORDER BY lb.block_height DESC
    `;
    return result;
  }

  async getSignerDataForBlock({ sql, blockId }: { sql: PgSqlClient; blockId: BlockIdParam }) {
    let blockFilter: Fragment;
    switch (blockId.type) {
      case 'height':
        blockFilter = sql`block_height = ${blockId.height}`;
        break;
      case 'hash':
        blockFilter = sql`block_hash = ${normalizeHexString(blockId.hash)}`;
        break;
      case 'latest':
        blockFilter = sql`block_height = (SELECT block_height FROM chain_tip)`;
        break;
      default:
        throw new Error(`Invalid blockId type: ${blockId}`);
    }

    const result = await sql<
      {
        block_height: number;
        block_hash: string;
        index_block_hash: string;
        burn_block_height: number;
        tenure_height: number;
        block_time: number;
        cycle_number: number | null;
        block_proposal_time_ms: string | null;
        total_signer_count: number;
        signer_accepted_mined_count: number;
        signer_accepted_excluded_count: number;
        signer_rejected_count: number;
        signer_missing_count: number;
        average_response_time_ms: number;
        accepted_mined_stacked_amount: string;
        accepted_excluded_stacked_amount: string;
        rejected_stacked_amount: string;
        missing_stacked_amount: string;
        accepted_mined_weight: number;
        accepted_excluded_weight: number;
        rejected_weight: number;
        missing_weight: number;
        chain_tip_block_height: number;
      }[]
    >`
      WITH latest_blocks AS (
        SELECT * FROM blocks
        WHERE ${blockFilter}
        LIMIT 1
      ),
      block_signers AS (
        SELECT
          lb.id AS block_id,
          lb.block_height,
          lb.block_time,
          lb.block_hash,
          lb.index_block_hash,
          lb.burn_block_height,
          bp.reward_cycle AS cycle_number,
          bp.received_at AS block_proposal_time_ms,
          rs.signer_key,
          COALESCE(rs.signer_weight, 0) AS signer_weight,
          COALESCE(rs.signer_stacked_amount, 0) AS signer_stacked_amount,
          CASE
            WHEN bss.id IS NOT NULL THEN 'accepted_mined'
            WHEN bss.id IS NULL AND fbr.accepted = TRUE THEN 'accepted_excluded'
            WHEN bss.id IS NULL AND fbr.accepted = FALSE THEN 'rejected'
            WHEN bss.id IS NULL AND fbr.id IS NULL THEN 'missing'
          END AS signer_status,
          EXTRACT(EPOCH FROM (fbr.received_at - bp.received_at)) * 1000 AS response_time_ms
        FROM latest_blocks lb
        LEFT JOIN block_proposals bp ON lb.block_hash = bp.block_hash
        LEFT JOIN reward_set_signers rs ON bp.reward_cycle = rs.cycle_number
        LEFT JOIN block_signer_signatures bss ON lb.block_height = bss.block_height AND rs.signer_key = bss.signer_key
        LEFT JOIN block_responses fbr ON fbr.signer_key = rs.signer_key AND fbr.signer_sighash = lb.block_hash
      ),
      signer_state_aggregation AS (
        SELECT
          block_id,
          MAX(cycle_number) AS cycle_number,
          MAX(block_proposal_time_ms) AS block_proposal_time_ms,
          COUNT(signer_key) AS total_signer_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'accepted_mined' THEN 1 END), 0) AS signer_accepted_mined_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'accepted_excluded' THEN 1 END), 0) AS signer_accepted_excluded_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'rejected' THEN 1 END), 0) AS signer_rejected_count,
          COALESCE(COUNT(CASE WHEN signer_status = 'missing' THEN 1 END), 0) AS signer_missing_count,
          COALESCE(AVG(response_time_ms) FILTER (WHERE signer_status IN ('accepted_mined', 'accepted_excluded', 'rejected')), 0) AS average_response_time_ms,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_mined' THEN signer_stacked_amount END), 0) AS accepted_mined_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_excluded' THEN signer_stacked_amount END), 0) AS accepted_excluded_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'rejected' THEN signer_stacked_amount END), 0) AS rejected_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'missing' THEN signer_stacked_amount END), 0) AS missing_stacked_amount,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_mined' THEN signer_weight END), 0) AS accepted_mined_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'accepted_excluded' THEN signer_weight END), 0) AS accepted_excluded_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'rejected' THEN signer_weight END), 0) AS rejected_weight,
          COALESCE(SUM(CASE WHEN signer_status = 'missing' THEN signer_weight END), 0) AS missing_weight
        FROM block_signers
        GROUP BY block_id
      )
      SELECT
        lb.block_height,
        lb.block_hash,
        lb.index_block_hash,
        lb.burn_block_height,
        lb.tenure_height,
        EXTRACT(EPOCH FROM lb.block_time)::integer AS block_time,
        bsa.cycle_number,
        (EXTRACT(EPOCH FROM bsa.block_proposal_time_ms) * 1000)::bigint AS block_proposal_time_ms,
        bsa.total_signer_count::integer,
        bsa.signer_accepted_mined_count::integer,
        bsa.signer_accepted_excluded_count::integer,
        bsa.signer_rejected_count::integer,
        bsa.signer_missing_count::integer,
        ROUND(bsa.average_response_time_ms, 3)::float8 AS average_response_time_ms,
        bsa.accepted_mined_stacked_amount,
        bsa.accepted_excluded_stacked_amount,
        bsa.rejected_stacked_amount,
        bsa.missing_stacked_amount,
        bsa.accepted_mined_weight::integer,
        bsa.accepted_excluded_weight::integer,
        bsa.rejected_weight::integer,
        bsa.missing_weight::integer,
        ct.block_height AS chain_tip_block_height
      FROM latest_blocks lb
      JOIN signer_state_aggregation bsa ON lb.id = bsa.block_id
      CROSS JOIN chain_tip ct
    `;
    if (result.length === 0) {
      return null;
    } else {
      return result[0];
    }
  }

  async getSignersForCycle({
    sql,
    cycleNumber,
    limit,
    offset,
    fromDate,
    toDate,
    signerKey,
  }: {
    sql: PgSqlClient;
    cycleNumber: number;
    limit: number;
    offset: number;
    signerKey?: string;
    // TODO: Do we need to support date ranges?
    fromDate?: Date;
    toDate?: Date;
  }) {
    // If no date range is provided, return the pre-calculated signer data for the given cycle number.
    if (fromDate === undefined && toDate === undefined) {
      return await sql<
        {
          signer_key: string;
          slot_index: number;
          weight: number;
          weight_percentage: number;
          stacked_amount: string;
          stacked_amount_percentage: number;
          stacked_amount_rank: number;
          proposals_accepted_count: number;
          proposals_rejected_count: number;
          proposals_missed_count: number;
          average_response_time_ms: number;
          last_block_response_time: Date | null;
          last_metadata_server_version: string | null;
        }[]
      >`
        SELECT
          signer_key,
          slot_index,
          signer_weight AS weight,
          ROUND(signer_weight_percentage::numeric * 100.0, 3)::float8 AS weight_percentage,
          signer_stacked_amount AS stacked_amount,
          ROUND(signer_stacked_amount_percentage::numeric * 100.0, 3)::float8 AS stacked_amount_percentage,
          signer_stacked_amount_rank AS stacked_amount_rank,
          proposals_accepted_count,
          proposals_rejected_count,
          proposals_missed_count,
          average_response_time_ms,
          last_response_time AS last_block_response_time,
          last_response_metadata_server_version AS last_metadata_server_version
        FROM reward_set_signers
        WHERE cycle_number = ${cycleNumber}
        ${signerKey ? sql`AND signer_key = ${normalizeHexString(signerKey)}` : sql``}
        ORDER BY signer_stacked_amount DESC, signer_key ASC
        OFFSET ${offset}
        LIMIT ${limit}
      `;
    }

    // Get the list of signers for a given cycle number via signer_key in the reward_set_signers table,
    // where cycle_number equals the given cycle number. Then get all block proposals from the block_proposals
    // table where reward_cycle matches the given cycle number. Each block_proposal has many associated
    // entries from the block_responses table (where block_proposal.block_hash matches block_responses.signer_sighash).
    // For each block_proposal there is an associated list of block_responses (at most one block_responses entry per signer_key).
    // For each signer_key (from the reward_set_signers table) get:
    //  * Number of block_proposal entries that have an associated accepted=true block_responses entry.
    //  * Number of block_proposal entries that have an associated accepted=false block_response entry.
    //  * Number of block_proposal entries that are missing an associated block_response entry.
    //  * The average time duration between block_proposal.received_at and block_response.received_at.
    // TODO: add pagination
    // TODO: joins against the block_signer_signatures table to determine mined_blocks_* values
    const fromFilter = fromDate ? sql`AND bp.received_at >= ${fromDate}` : sql``;
    const toFilter = toDate ? sql`AND bp.received_at < ${toDate}` : sql``;

    const dbRewardSetSigners = await sql<
      {
        signer_key: string;
        slot_index: number;
        weight: number;
        weight_percentage: number;
        stacked_amount: string;
        stacked_amount_percentage: number;
        stacked_amount_rank: number;
        proposals_accepted_count: number;
        proposals_rejected_count: number;
        proposals_missed_count: number;
        average_response_time_ms: number;
        last_block_response_time: Date | null;
        last_metadata_server_version: string | null;
      }[]
    >`
      WITH signer_data AS (
        -- Fetch the signers for the given cycle
        SELECT
          rss.signer_key,
          rss.slot_index,
          rss.signer_weight,
          rss.signer_stacked_amount
        FROM reward_set_signers rss
        WHERE rss.cycle_number = ${cycleNumber}
      ),
      proposal_data AS (
        -- Select all proposals for the given cycle
        SELECT
          bp.block_hash,
          bp.block_height,
          bp.received_at AS proposal_received_at
        FROM block_proposals bp
        WHERE 
          bp.reward_cycle = ${cycleNumber}
          ${fromFilter}
          ${toFilter}
      ),
      response_data AS (
        -- Select responses associated with the proposals from the given cycle
        SELECT
          br.signer_key,
          br.signer_sighash,
          br.accepted,
          br.received_at,
          br.metadata_server_version,
          br.id
        FROM block_responses br
        JOIN proposal_data pd ON br.signer_sighash = pd.block_hash -- Only responses linked to selected proposals
      ),
      latest_response_data AS (
        -- Find the latest response time and corresponding metadata_server_version for each signer
        SELECT DISTINCT ON (signer_key)
          signer_key,
          received_at AS last_block_response_time,
          metadata_server_version
        FROM response_data
        ORDER BY signer_key, received_at DESC
      ),
      signer_proposal_data AS (
        -- Cross join signers with proposals and left join filtered responses
        SELECT
          sd.signer_key,
          pd.block_hash,
          pd.proposal_received_at,
          rd.accepted,
          rd.received_at AS response_received_at,
          EXTRACT(EPOCH FROM (rd.received_at - pd.proposal_received_at)) * 1000 AS response_time_ms
        FROM signer_data sd
        CROSS JOIN proposal_data pd -- Cross join to associate all signers with all proposals
        LEFT JOIN response_data rd
          ON pd.block_hash = rd.signer_sighash
          AND sd.signer_key = rd.signer_key -- Match signers with their corresponding responses
      ),
      aggregated_data AS (
        -- Aggregate the proposal and response data by signer
        SELECT
          spd.signer_key,
          COUNT(CASE WHEN spd.accepted = true THEN 1 END)::integer AS proposals_accepted_count,
          COUNT(CASE WHEN spd.accepted = false THEN 1 END)::integer AS proposals_rejected_count,
          COUNT(CASE WHEN spd.accepted IS NULL THEN 1 END)::integer AS proposals_missed_count,
          ROUND(AVG(spd.response_time_ms), 3)::float8 AS average_response_time_ms
        FROM signer_proposal_data spd
        GROUP BY spd.signer_key
      ),
      signer_rank AS (
        -- Calculate the rank of each signer based on stacked amount
        SELECT
          signer_key,
          RANK() OVER (ORDER BY signer_stacked_amount DESC) AS stacked_amount_rank
        FROM reward_set_signers
        WHERE cycle_number = ${cycleNumber}
      )
      SELECT
        sd.signer_key,
        sd.slot_index,
        sd.signer_weight AS weight,
        sd.signer_stacked_amount AS stacked_amount,
        ROUND(sd.signer_weight * 100.0 / (SELECT SUM(signer_weight) FROM reward_set_signers WHERE cycle_number = ${cycleNumber}), 3)::float8 AS weight_percentage,
        ROUND(sd.signer_stacked_amount * 100.0 / (SELECT SUM(signer_stacked_amount) FROM reward_set_signers WHERE cycle_number = ${cycleNumber}), 3)::float8 AS stacked_amount_percentage,
        sr.stacked_amount_rank,
        ad.proposals_accepted_count,
        ad.proposals_rejected_count,
        ad.proposals_missed_count,
        COALESCE(ad.average_response_time_ms, 0) AS average_response_time_ms,
        COALESCE(lrd.last_block_response_time, NULL) AS last_block_response_time,
        COALESCE(lrd.metadata_server_version, NULL) AS last_metadata_server_version
      FROM signer_data sd
      LEFT JOIN aggregated_data ad
        ON sd.signer_key = ad.signer_key
      LEFT JOIN signer_rank sr
        ON sd.signer_key = sr.signer_key
      LEFT JOIN latest_response_data lrd
        ON sd.signer_key = lrd.signer_key -- Join the latest response time and metadata server version data
      ORDER BY sd.signer_stacked_amount DESC, sd.signer_key ASC
    `;
    return dbRewardSetSigners;
  }

  async getCurrentCycleSignersWeightPercentage() {
    return await this.sql<{ signer_key: string; weight: number }[]>`
      SELECT signer_key, ROUND(signer_weight_percentage::numeric * 100.0, 3)::float AS weight
      FROM reward_set_signers
      WHERE cycle_number = (SELECT MAX(cycle_number) FROM reward_set_signers)
      ORDER BY signer_weight_percentage DESC, signer_key ASC
    `;
  }

  async getSignerForCycle(cycleNumber: number, signerId: string) {
    const dbRewardSetSigner = await this.getSignersForCycle({
      sql: this.sql,
      cycleNumber,
      signerKey: signerId,
      limit: 1,
      offset: 0,
    });
    return dbRewardSetSigner.length > 0 ? dbRewardSetSigner[0] : null;
  }

  async getRecentSignerMetrics(args: { sql: PgSqlClient; blockRanges: number[] }) {
    const result = await args.sql<
      {
        signer_key: string;
        block_ranges: {
          [blocks: string]: {
            missing: number;
            accepted: number;
            rejected: number;
          };
        };
      }[]
    >`
      WITH block_ranges AS (
        SELECT unnest(${args.blockRanges}::integer[]) AS range_value
      ),
      latest_block_proposal AS (
        SELECT *
        FROM block_proposals
        ORDER BY received_at DESC
        LIMIT 1
      ),
      signer_keys AS (
        SELECT rs.signer_key
        FROM reward_set_signers rs
        JOIN latest_block_proposal lbp ON rs.cycle_number = lbp.reward_cycle
      ),
      recent_blocks AS (
        SELECT
          bp.block_hash,
          bp.received_at,
          ROW_NUMBER() OVER (ORDER BY bp.received_at DESC) AS row_num
        FROM block_proposals bp
        ORDER BY bp.received_at DESC
        LIMIT (SELECT MAX(range_value) FROM block_ranges)
      ),
      filtered_blocks AS (
        SELECT
          rb.block_hash,
          sk.signer_key,
          br.range_value AS block_range
        FROM block_ranges br
        CROSS JOIN signer_keys sk
        JOIN recent_blocks rb
          ON rb.row_num <= br.range_value
      ),
      -- Filter block_responses with a received_at older than the oldest recent_blocks timestamp
      block_responses_after AS (
        SELECT * FROM block_responses br
        WHERE br.received_at >= (
          SELECT MIN(received_at) - INTERVAL '1 hour' FROM recent_blocks
        )
      ),
      -- Filter block_responses with hashes not included in recent_blocks, or with signer_keys not included in this cycle
      relevant_responses AS (
        SELECT br.signer_key, br.signer_sighash, br.accepted
        FROM block_responses_after br
        JOIN recent_blocks rb ON br.signer_sighash = rb.block_hash
        JOIN signer_keys sk ON br.signer_key = sk.signer_key
      ),
      responses_with_states AS (
        SELECT
          fb.signer_key,
          fb.block_range,
          COALESCE(
            CASE
              WHEN rr.accepted IS TRUE THEN 'accepted'
              WHEN rr.accepted IS FALSE THEN 'rejected'
            END, 'missing'
          ) AS response_state,
          COUNT(*) AS state_count
        FROM filtered_blocks fb
        LEFT JOIN relevant_responses rr
          ON fb.block_hash = rr.signer_sighash AND fb.signer_key = rr.signer_key
        GROUP BY fb.signer_key, fb.block_range, response_state
      ),
      aggregated_counts AS (
        SELECT
          signer_key,
          block_range,
          MAX(CASE WHEN response_state = 'accepted' THEN state_count ELSE 0 END) AS accepted,
          MAX(CASE WHEN response_state = 'rejected' THEN state_count ELSE 0 END) AS rejected,
          MAX(CASE WHEN response_state = 'missing' THEN state_count ELSE 0 END) AS missing
        FROM responses_with_states
        GROUP BY signer_key, block_range
      )
      SELECT
        signer_key,
        jsonb_object_agg(block_range::text, jsonb_build_object(
          'accepted', accepted,
          'rejected', rejected,
          'missing', missing
        )) AS block_ranges
      FROM aggregated_counts
      GROUP BY signer_key
      ORDER BY signer_key, block_ranges
    `;
    return result;
  }

  async getRecentBlockPushMetrics(args: { sql: PgSqlClient; blockRanges: number[] }) {
    const result = await args.sql<
      {
        block_range: number;
        avg_push_time_ms: number;
      }[]
    >`
      WITH block_ranges AS (
        SELECT unnest(${args.blockRanges}::integer[]) AS range_value
      ),
      recent_block_pushes AS (
        SELECT
          bp.block_hash,
          bp.received_at AS push_received_at,
          ROW_NUMBER() OVER (ORDER BY bp.received_at DESC) AS row_num
        FROM block_pushes bp
        ORDER BY bp.received_at DESC
        LIMIT (SELECT MAX(range_value) FROM block_ranges)
      ),
      joined_blocks AS (
        SELECT
          rbp.row_num,
          br.range_value AS block_range,
          EXTRACT(EPOCH FROM (rbp.push_received_at - bp.received_at)) * 1000 AS push_time_ms
        FROM block_ranges br
        JOIN recent_block_pushes rbp
          ON rbp.row_num <= br.range_value
        JOIN block_proposals bp
          ON rbp.block_hash = bp.block_hash
      )
      SELECT
        block_range,
        ROUND(AVG(push_time_ms), 3)::float8 AS avg_push_time_ms
      FROM joined_blocks
      GROUP BY block_range
      ORDER BY block_range
    `;
    return result;
  }

  async getRecentBlockAcceptanceMetrics(args: { sql: PgSqlClient; blockRanges: number[] }) {
    const result = await args.sql<
      {
        block_range: number;
        acceptance_rate: number;
      }[]
    >`
      WITH block_ranges AS (
        SELECT unnest(${args.blockRanges}::integer[]) AS range_value
      ),
      recent_block_proposals AS (
        SELECT
          bp.block_hash,
          ROW_NUMBER() OVER (ORDER BY bp.received_at DESC) AS row_num
        FROM block_proposals bp
        ORDER BY bp.received_at DESC
        LIMIT (SELECT MAX(range_value) FROM block_ranges)
      ),
      filtered_proposals AS (
        SELECT
          rbp.block_hash,
          br.range_value AS block_range
        FROM block_ranges br
        JOIN recent_block_proposals rbp
          ON rbp.row_num <= br.range_value
      ),
      proposal_push_matches AS (
        SELECT
          fp.block_range,
          COUNT(fp.block_hash) AS total_proposals,
          COUNT(bp.block_hash) AS accepted_proposals
        FROM filtered_proposals fp
        LEFT JOIN block_pushes bp
          ON fp.block_hash = bp.block_hash
        GROUP BY fp.block_range
      ),
      acceptance_rates AS (
        SELECT
          block_range,
          ROUND(COALESCE(accepted_proposals::float8 / total_proposals::float8, 0)::numeric, 4)::float8 AS acceptance_rate
        FROM proposal_push_matches
      )
      SELECT *
      FROM acceptance_rates
      ORDER BY block_range
    `;
    return result;
  }

  async getPendingProposalDate(args: { sql: PgSqlClient; kind: 'oldest' | 'newest' }) {
    const result = await args.sql<
      {
        received_at: Date | null;
      }[]
    >`
      WITH max_heights AS (
        SELECT 
          GREATEST(
            (SELECT COALESCE(MAX(block_height), 0) FROM blocks),
            (SELECT COALESCE(MAX(block_height), 0) FROM block_pushes)
          ) AS height
      ),
      pending_proposal AS (
        SELECT received_at
        FROM block_proposals bp
        WHERE bp.block_height > (SELECT height FROM max_heights)
        ORDER BY received_at ${args.kind == 'newest' ? args.sql`DESC` : args.sql`ASC`}
        LIMIT 1
      )
      SELECT received_at
      FROM pending_proposal
    `;

    // Return the computed value or null if no pending proposals exist
    return result.length > 0 ? result[0].received_at : null;
  }
}
