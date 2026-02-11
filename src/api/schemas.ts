import { SwaggerOptions } from '@fastify/swagger';
import { has0xPrefix, SERVER_VERSION } from '@hirosystems/api-toolkit';
import { Static, Type } from '@sinclair/typebox';
import { BlockIdParam } from '../helpers';

export const OpenApiSchemaOptions: SwaggerOptions = {
  openapi: {
    info: {
      title: 'Signer Metrics API',
      description: 'Welcome to the API reference overview for the Signer Metrics API.',
      version: SERVER_VERSION.tag,
    },
    externalDocs: {
      url: 'https://github.com/hirosystems/signer-metrics-api',
      description: 'Source Repository',
    },
    servers: [
      {
        url: 'https://api.hiro.so/',
        description: 'mainnet',
      },
      {
        url: 'https://api.testnet.hiro.so/',
        description: 'testnet',
      },
    ],
    tags: [
      {
        name: 'Status',
        description: 'Service status endpoints',
      },
    ],
  },
};

export const ApiStatusResponse = Type.Object(
  {
    server_version: Type.String({ examples: ['signer-metrics-api v0.0.1 (master:a1b2c3)'] }),
    status: Type.String({ examples: ['ready'] }),
    chain_tip: Type.Object({
      block_height: Type.Integer({ examples: [163541] }),
      index_block_hash: Type.String({ examples: ['0x1234567890abcdef'] }),
    }),
  },
  { title: 'Api Status Response' }
);

export const BlocksEntrySignerDataSchema = Type.Object({
  cycle_number: Type.Integer(),
  total_signer_count: Type.Integer({
    description: 'Total number of signers expected for this block',
  }),

  accepted_count: Type.Integer({
    description: 'Number of signers that submitted an approval for this block',
  }),
  rejected_count: Type.Integer({
    description: 'Number of signers that submitted a rejection for this block',
  }),
  missing_count: Type.Integer({
    description: 'Number of signers that failed to submit any response/vote for this block',
  }),

  accepted_excluded_count: Type.Integer({
    description:
      'Number of signers that submitted an approval but where not included in time by the miner (this is a subset of the accepted_count)',
  }),

  average_response_time_ms: Type.Number({
    description:
      'Average time duration (in milliseconds) taken by signers to submit a response for this block (tracked best effort)',
  }),
  block_proposal_time_ms: Type.Number({
    description:
      'Unix timestamp in milliseconds of when the block was first proposed (tracked best effort)',
  }),

  accepted_stacked_amount: Type.String({
    description: 'Sum of total STX stacked of signers who approved the block',
  }),
  rejected_stacked_amount: Type.String({
    description: 'Sum of total STX stacked of signers who rejected the block',
  }),
  missing_stacked_amount: Type.String({
    description: 'Sum of total STX stacked of missing signers',
  }),

  accepted_weight: Type.Integer({
    description:
      'Sum of voting weight of signers who approved the block (based on slots allocated to each signer proportional to stacked amount)',
  }),
  rejected_weight: Type.Integer({
    description: 'Sum of voting weight of signers who rejected the block',
  }),
  missing_weight: Type.Integer({
    description: 'Sum of voting weight of missing signers',
  }),
});

export type BlocksEntrySignerData = Static<typeof BlocksEntrySignerDataSchema>;

export const BlockEntrySchema = Type.Object({
  block_height: Type.Integer(),
  block_hash: Type.String(),
  block_time: Type.Integer({
    description: 'Unix timestamp in seconds of when the block was mined',
  }),
  index_block_hash: Type.String(),
  burn_block_height: Type.Integer(),
  tenure_height: Type.Integer(),
  signer_data: Type.Union([BlocksEntrySignerDataSchema, Type.Null()], {
    description: 'Signer data can by null if it was not detected by the metrics service',
  }),
});
export type BlocksEntry = Static<typeof BlockEntrySchema>;

export const BlocksResponseSchema = Type.Object({
  total: Type.Integer(),
  // TODO: implement cursor pagination
  // next_cursor: Type.String(),
  // prev_cursor: Type.String(),
  // cursor: Type.String(),
  limit: Type.Integer(),
  offset: Type.Integer(),
  results: Type.Array(BlockEntrySchema),
});

export type BlocksResponse = Static<typeof BlocksResponseSchema>;

export const CycleSignerSchema = Type.Object({
  signer_key: Type.String(),
  slot_index: Type.Integer({ description: 'Index of the signer in the stacker set' }),
  weight: Type.Integer({
    description:
      'Voting weight of this signer (based on slots allocated which is proportional to stacked amount)',
  }),
  weight_percentage: Type.Number({
    description: 'Voting weight percent (weight / total_weight)',
  }),
  stacked_amount: Type.String({
    description: 'Total STX stacked associated with this signer (string quoted integer)',
  }),
  stacked_amount_percent: Type.Number({
    description: 'Stacked amount percent (stacked_amount / total_stacked_amount)',
  }),
  stacked_amount_rank: Type.Integer({
    description:
      "This signer's rank in the list of all signers (for this cycle) ordered by stacked amount",
  }),
  proposals_accepted_count: Type.Integer({
    description: 'Number of block proposals accepted by this signer',
  }),
  proposals_rejected_count: Type.Integer({
    description: 'Number of block proposals rejected by this signer',
  }),
  proposals_missed_count: Type.Integer({
    description: 'Number of block proposals missed by this signer',
  }),
  average_response_time_ms: Type.Number({
    description:
      'Time duration (in milliseconds) taken to submit responses to block proposals (tracked best effort)',
  }),
  last_seen: Type.Union([Type.String(), Type.Null()], {
    description: 'ISO timestamp of the last time a message from this signer was seen',
  }),
  version: Type.Union([Type.String(), Type.Null()], {
    description: 'The last seen signer binary version reported by this signer',
  }),
  // TODO: implement these nice-to-have fields
  /*
  mined_blocks_accepted_included_count: Type.Integer({
    description: 'Number of mined blocks where signer approved and was included',
  }),
  mined_blocks_accepted_excluded_count: Type.Integer({
    description: 'Number of mined blocks where signer approved but was not included',
  }),
  mined_blocks_rejected_count: Type.Integer({
    description: 'Number of mined blocks where signer rejected',
  }),
  mined_blocks_missing_count: Type.Integer({
    description: 'Number of mined blocks where signer was missing',
  }),
  */
});
export type CycleSigner = Static<typeof CycleSignerSchema>;

export const CycleSignersResponseSchema = Type.Object({
  total: Type.Integer(),
  // TODO: implement cursor pagination
  // next_cursor: Type.String(),
  // prev_cursor: Type.String(),
  // cursor: Type.String(),
  limit: Type.Integer(),
  offset: Type.Integer(),
  results: Type.Array(CycleSignerSchema),
});
export type CycleSignersResponse = Static<typeof CycleSignersResponseSchema>;

export const CycleSignerResponseSchema = Type.Composite([CycleSignerSchema]);
export type CycleSignerResponse = Static<typeof CycleSignerResponseSchema>;

export const BlockHashParamSchema = Type.String({
  pattern: '^(0x)?[a-fA-F0-9]{64}$',
  title: 'Block hash',
  description: 'Block hash',
  examples: ['0xdaf79950c5e8bb0c620751333967cdd62297137cdaf79950c5e8bb0c62075133'],
});

const BlockHeightParamSchema = Type.Integer({
  title: 'Block height',
  description: 'Block height',
  examples: [777678],
});

export const BlockParamsSchema = Type.Object(
  {
    height_or_hash: Type.Union([
      Type.Literal('latest'),
      BlockHashParamSchema,
      BlockHeightParamSchema,
    ]),
  },
  { additionalProperties: false }
);
export type BlockParams = Static<typeof BlockParamsSchema>;

/**
 * If a param can accept a block hash or height, then ensure that the hash is prefixed with '0x' so
 * that hashes with only digits are not accidentally parsed as a number.
 */
export function cleanBlockHeightOrHashParam(params: { height_or_hash: string | number }) {
  if (
    typeof params.height_or_hash === 'string' &&
    /^[a-fA-F0-9]{64}$/i.test(params.height_or_hash)
  ) {
    params.height_or_hash = '0x' + params.height_or_hash;
  }
}

export function parseBlockParam(value: BlockParams['height_or_hash']): BlockIdParam {
  if (value === 'latest') {
    return { type: 'latest', latest: true };
  }
  value = typeof value === 'string' ? value : value.toString();
  if (/^(0x)?[a-fA-F0-9]{64}$/i.test(value)) {
    return { type: 'hash', hash: has0xPrefix(value) ? value : `0x${value}` };
  }
  if (/^[0-9]+$/.test(value)) {
    return { type: 'height', height: parseInt(value) };
  }
  throw new Error('Invalid block height or hash');
}

export const BlockProposalSignerDataSchema = Type.Object({
  signer_key: Type.String(),
  slot_index: Type.Integer({ description: 'Index of the signer in the stacker set' }),
  response: Type.Union([
    Type.Literal('accepted'),
    Type.Literal('rejected'),
    Type.Literal('missing'),
  ]),
  weight: Type.Integer({
    description:
      'Voting weight of this signer (based on slots allocated which is proportional to stacked amount)',
  }),
  weight_percentage: Type.Number({
    description: 'Voting weight percent (weight / total_weight)',
  }),
  stacked_amount: Type.String({
    description: 'Total STX stacked associated with this signer (string quoted integer)',
  }),
  version: Type.Union([Type.String(), Type.Null()], {
    description:
      'The signer binary version reported by this signer for this proposal response (null if response missing)',
  }),
  received_at: Type.Union([Type.String(), Type.Null()], {
    description: 'ISO timestamp of when this response was received (null if response missing',
  }),
  response_time_ms: Type.Union([Type.Integer(), Type.Null()], {
    description:
      'Time duration (in milliseconds) taken to submit this response (tracked best effort, null if response missing)',
  }),
  reason_string: Type.Union([Type.String(), Type.Null()], {
    description: '(For rejection responses) reason string for rejection',
  }),
  reason_code: Type.Union([Type.String(), Type.Null()], {
    description: '(For rejection responses) reason code for rejection',
  }),
  reject_code: Type.Union([Type.String(), Type.Null()], {
    description: '(For rejection responses) reject code for rejection',
  }),
});
export type BlockProposalSignerData = Static<typeof BlockProposalSignerDataSchema>;

export const BlockProposalsEntrySchema = Type.Object({
  received_at: Type.String({
    description: 'ISO timestamp of when this block proposal was received',
  }),
  block_height: Type.Integer(),
  block_hash: Type.String(),
  index_block_hash: Type.String(),
  burn_block_height: Type.Integer(),
  block_time: Type.Integer({
    description: 'Unix timestamp in seconds of when the block was mined',
  }),
  cycle_number: Type.Integer(),

  status: Type.Union(
    [Type.Literal('pending'), Type.Literal('accepted'), Type.Literal('rejected')],
    {
      description: 'Status of the block proposal',
    }
  ),

  push_time_ms: Type.Union([Type.Null(), Type.Integer()], {
    description:
      'Time duration (in milliseconds) taken between when the block was proposed and when it was pushed',
  }),

  // cycle data
  total_signer_count: Type.Integer({
    description: 'Total number of signers expected for this proposal',
  }),
  total_signer_weight: Type.Integer({
    description: 'Total voting weight of signers expected for this proposal',
  }),
  total_signer_stacked_amount: Type.String({
    description: 'Total STX stacked of signers expected for this proposal',
  }),

  accepted_count: Type.Integer({
    description: 'Number of signers that submitted an approval for this proposal',
  }),
  rejected_count: Type.Integer({
    description: 'Number of signers that submitted a rejection for this proposal',
  }),
  missing_count: Type.Integer({
    description: 'Number of signers that failed to submit any response/vote for this proposal',
  }),

  accepted_weight: Type.Integer({
    description: 'Sum of voting weight of signers who approved the proposal',
  }),
  rejected_weight: Type.Integer({
    description: 'Sum of voting weight of signers who rejected the proposal',
  }),
  missing_weight: Type.Integer({
    description: 'Sum of voting weight of missing signers',
  }),

  // signer responses
  signer_data: Type.Array(BlockProposalSignerDataSchema),
});
export type BlockProposalsEntry = Static<typeof BlockProposalsEntrySchema>;

export const BlockProposalsResponseSchema = Type.Object({
  // TODO: implement cursor pagination
  // next_cursor: Type.String(),
  // prev_cursor: Type.String(),
  // cursor: Type.String(),
  limit: Type.Integer(),
  offset: Type.Integer(),
  results: Type.Array(BlockProposalsEntrySchema),
});
export type BlockProposalsResponse = Static<typeof BlockProposalsResponseSchema>;
