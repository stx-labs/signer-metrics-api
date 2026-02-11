import { Gauge } from 'prom-client';
import { ENV } from './env';
import { PgStore } from './pg/pg-store';

export function configureSignerMetrics(db: PgStore) {
  // Getter for block periods so that the env var can be updated
  const getBlockPeriods = () => ENV.SIGNER_PROMETHEUS_METRICS_BLOCK_PERIODS.split(',').map(Number);

  const metricsPrefix = 'signer_api_';

  new Gauge({
    name: metricsPrefix + 'time_since_oldest_pending_block_proposal_ms',
    help: 'Time in milliseconds since the oldest pending block proposal',
    async collect() {
      const dbResult = await db.sqlTransaction(async sql => {
        return await db.getPendingProposalDate({ sql, kind: 'oldest' });
      });
      this.reset();
      this.set(dbResult ? Date.now() - dbResult.getTime() : 0);
    },
  });

  new Gauge({
    name: metricsPrefix + 'time_since_newest_pending_block_proposal_ms',
    help: 'Time in milliseconds since the most recent pending block proposal',
    async collect() {
      const dbResult = await db.sqlTransaction(async sql => {
        return await db.getPendingProposalDate({ sql, kind: 'newest' });
      });
      this.reset();
      this.set(dbResult ? Date.now() - dbResult.getTime() : 0);
    },
  });

  new Gauge({
    name: metricsPrefix + 'avg_block_push_time_ms',
    help: 'Average time (in milliseconds) taken for block proposals to be accepted and pushed over different block periods',
    labelNames: ['period'] as const,
    async collect() {
      const dbResults = await db.sqlTransaction(async sql => {
        return await db.getRecentBlockPushMetrics({ sql, blockRanges: getBlockPeriods() });
      });
      this.reset();
      for (const row of dbResults) {
        this.set({ period: row.block_range }, row.avg_push_time_ms);
      }
    },
  });

  new Gauge({
    name: metricsPrefix + 'proposal_acceptance_rate',
    help: 'The acceptance rate of block proposals for different block ranges (as a float between 0 and 1).',
    labelNames: ['period'],
    async collect() {
      const dbResults = await db.sqlTransaction(async sql => {
        return await db.getRecentBlockAcceptanceMetrics({ sql, blockRanges: getBlockPeriods() });
      });
      this.reset();
      for (const row of dbResults) {
        this.set({ period: row.block_range }, row.acceptance_rate);
      }
    },
  });

  new Gauge({
    name: metricsPrefix + 'signer_state_count',
    help: 'Count of signer states over different block periods',
    labelNames: ['signer', 'period', 'state'] as const,
    async collect() {
      const dbResults = await db.sqlTransaction(async sql => {
        return await db.getRecentSignerMetrics({ sql, blockRanges: getBlockPeriods() });
      });
      this.reset();
      for (const row of dbResults) {
        for (const [blockRange, states] of Object.entries(row.block_ranges)) {
          for (const [state, count] of Object.entries(states)) {
            this.set({ signer: row.signer_key, period: blockRange, state: state }, count);
          }
        }
      }
    },
  });

  new Gauge({
    name: metricsPrefix + 'signer_weight_percentage',
    help: 'Signer weight percentage for the current cycle',
    labelNames: ['signer'] as const,
    async collect() {
      const dbResults = await db.getCurrentCycleSignersWeightPercentage();
      this.reset();
      for (const row of dbResults) {
        this.set({ signer: row.signer_key }, row.weight);
      }
    },
  });

  new Gauge({
    name: metricsPrefix + 'stacks_block_height',
    help: 'Last indexed stacks block height',
    async collect() {
      const chainTip = await db.getChainTip(db.sql);
      this.reset();
      this.set(chainTip.block_height);
    },
  });
}
