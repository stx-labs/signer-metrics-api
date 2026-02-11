import { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import { FastifyPluginCallback } from 'fastify';
import { Server } from 'http';
import { ApiStatusResponse } from '../schemas';
import { SERVER_VERSION } from '@hirosystems/api-toolkit';

export const StatusRoutes: FastifyPluginCallback<
  Record<never, never>,
  Server,
  TypeBoxTypeProvider
> = (fastify, _options, done) => {
  fastify.get(
    '/',
    {
      schema: {
        operationId: 'getApiStatus',
        summary: 'API Status',
        description: 'Displays the status of the API and its current workload',
        tags: ['Status'],
        response: {
          200: ApiStatusResponse,
        },
      },
    },
    async (_req, reply) => {
      const result = await fastify.db.sqlTransaction(async _sql => {
        const chainTip = await fastify.db.getChainTip(fastify.db.sql);

        return {
          server_version: `signer-metrics-api ${SERVER_VERSION.tag} (${SERVER_VERSION.branch}:${SERVER_VERSION.commit})`,
          status: 'ready',
          chain_tip: {
            block_height: chainTip.block_height,
            index_block_hash: chainTip.index_block_hash,
          },
        };
      });
      await reply.send(result);
    }
  );
  done();
};
