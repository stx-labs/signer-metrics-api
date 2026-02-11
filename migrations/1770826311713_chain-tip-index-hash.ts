/* eslint-disable @typescript-eslint/naming-convention */
import { MigrationBuilder, ColumnDefinitions } from 'node-pg-migrate';

export const shorthands: ColumnDefinitions | undefined = undefined;

export function up(pgm: MigrationBuilder): void {
  pgm.addColumn('chain_tip', {
    index_block_hash: {
      type: 'bytea',
    },
  });
  pgm.sql(`UPDATE chain_tip SET index_block_hash = '\\x'`);
  pgm.alterColumn('chain_tip', 'index_block_hash', {
    notNull: true,
  });
  pgm.dropColumn('chain_tip', 'last_redis_msg_id');
}

export function down(pgm: MigrationBuilder): void {
  pgm.dropColumn('chain_tip', 'index_block_hash');
  pgm.addColumn('chain_tip', {
    last_redis_msg_id: {
      type: 'text',
      notNull: true,
      default: '0',
    },
  });
}
