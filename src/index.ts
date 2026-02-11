import { PgStore } from './pg/pg-store';
import { buildApiServer, buildPromServer } from './api/init';
import { ENV } from './env';
import { isProdEnv } from './helpers';
import { buildProfilerServer, logger, registerShutdownConfig } from '@hirosystems/api-toolkit';
import { EventStreamHandler } from './event-stream/event-stream';
import { startPoxInfoUpdater } from './stacks-core-rpc/pox-info-updater';
import { StackerSetUpdator } from './stacks-core-rpc/stacker-set-updater';
import { configureSignerMetrics } from './prom-metrics';

/**
 * Initializes background services. Only for `default` and `writeonly` run modes.
 * @param db - PgStore
 */
async function initBackgroundServices(db: PgStore) {
  logger.info('Initializing background services...');

  const poxInfoUpdater = startPoxInfoUpdater({ db });
  registerShutdownConfig({
    name: 'stacks-core RPC PoX fetcher',
    forceKillable: false,
    handler: () => {
      poxInfoUpdater.close();
    },
  });

  const stackerSetUpdater = new StackerSetUpdator({ db });
  registerShutdownConfig({
    name: 'StackerSet fetcher',
    forceKillable: false,
    handler: async () => {
      await stackerSetUpdater.stop();
    },
  });

  const chainTip = await db.getChainTip(db.sql);
  logger.info(
    `signer-metrics-api is at block ${chainTip.block_height}, index_block_hash: ${chainTip.index_block_hash}`
  );
  const eventStreamListener = new EventStreamHandler({ db });
  registerShutdownConfig({
    name: 'Redis Event Stream',
    forceKillable: false,
    handler: async () => {
      await eventStreamListener.stop();
    },
  });
  await eventStreamListener.start();
}

/**
 * Initializes API service. Only for `default` and `readonly` run modes.
 * @param db - PgStore
 */
async function initApiService(db: PgStore) {
  logger.info('Initializing API service...');
  const apiServer = await buildApiServer({ db });
  registerShutdownConfig({
    name: 'API Server',
    forceKillable: false,
    handler: async () => {
      await apiServer.close();
    },
  });

  await apiServer.listen({ host: ENV.API_HOST, port: ENV.API_PORT });

  if (isProdEnv) {
    const promServer = await buildPromServer({ metrics: apiServer.metrics });
    registerShutdownConfig({
      name: 'Prometheus Server',
      forceKillable: false,
      handler: async () => {
        await promServer.close();
      },
    });
    configureSignerMetrics(db);
    await promServer.listen({ host: ENV.API_HOST, port: ENV.PROMETHEUS_PORT });
  }
}

async function initApp() {
  logger.info(`Initializing in ${ENV.RUN_MODE} run mode...`);
  const isReadonly = ENV.RUN_MODE === 'readonly';
  const db = await PgStore.connect({
    skipMigrations: isReadonly,
    createSchema: !isReadonly,
  });

  if (['default', 'writeonly'].includes(ENV.RUN_MODE)) {
    await initBackgroundServices(db);
  }
  if (['default', 'readonly'].includes(ENV.RUN_MODE)) {
    await initApiService(db);
  }

  const profilerServer = await buildProfilerServer();
  registerShutdownConfig({
    name: 'Profiler Server',
    forceKillable: false,
    handler: async () => {
      await profilerServer.close();
    },
  });
  await profilerServer.listen({ host: ENV.API_HOST, port: ENV.PROFILER_PORT });

  registerShutdownConfig({
    name: 'DB',
    forceKillable: false,
    handler: async () => {
      await db.close();
    },
  });
}

registerShutdownConfig();
initApp()
  .then(() => {
    logger.info('App initialized');
  })
  .catch((error: unknown) => {
    logger.error(error, `App failed to start`);
    process.exit(1);
  });
