import { StackerDbChunksMessage } from '@stacks/node-publisher-client';

describe('Chunk parsing tests', () => {
  // Jest has an issue with loading the `@noble/secp256k1` library related to esm limitations in jest
  test.skip('parse block response with ValidationFailed(PubkeyHashMismatch)', async () => {
    const { parseStackerDbChunk } = await import('../../src/event-stream/msg-parsing');
    const payload = `{"contract_id":{"issuer":[22,[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]],"name":"signers-1-1"},"modified_slots":[{"data":"010100000038426c6f636b206973206e6f7420686967686572207468616e20746865206869676865737420626c6f636b20696e206974732074656e7572650008257441ee3785f859ed3a302f2ee19b4ecb3fc3e494c9f982011d6a49a29b36df0000000100f82c5e8637fe8eaf0cee174700841e921c7c16b6163b3804a780f7cbfe35fa8b79a3a2f22878dd66caff6f2ba891b647991147326bbb10de079759edcda3142a00000055737461636b732d7369676e657220332e312e302e302e392e30202872656c656173652f332e312e302e302e393a636262323264362b2c2072656c65617365206275696c642c206c696e7578205b7838365f36345d29030000000a00000000681bbe160008","sig":"01031fe2a0528a2cea71b5cc23e660b5ccddadb730caee4cc0a8388d6df8c5c8fa52d75d243c840c2f34bbc68878b63e7bbb2a6fb0711a843d37f390d366f6d0b4","slot_id":10,"slot_version":176385}]}`;
    const payloadJson: StackerDbChunksMessage = JSON.parse(payload);
    const parsed = parseStackerDbChunk(payloadJson);
    expect(parsed[0]).toMatchObject({
      contract: 'signers-1-1',
      pubkey: '025588e24e2bf387fe8cc7bccba1aac7fe599b96724892431e992a40d06e8fe220',
      sig: '01031fe2a0528a2cea71b5cc23e660b5ccddadb730caee4cc0a8388d6df8c5c8fa52d75d243c840c2f34bbc68878b63e7bbb2a6fb0711a843d37f390d366f6d0b4',
      messageType: 'BlockResponse',
      blockResponse: {
        type: 'rejected',
        reason: 'Block is not higher than the highest block in its tenure',
        reasonCode: { rejectCode: 'ValidationFailed', validateRejectCode: 'PubkeyHashMismatch' },
        signerSignatureHash: '257441ee3785f859ed3a302f2ee19b4ecb3fc3e494c9f982011d6a49a29b36df',
        chainId: 1,
        signature:
          '00f82c5e8637fe8eaf0cee174700841e921c7c16b6163b3804a780f7cbfe35fa8b79a3a2f22878dd66caff6f2ba891b647991147326bbb10de079759edcda3142a',
        metadata: {
          server_version:
            'stacks-signer 3.1.0.0.9.0 (release/3.1.0.0.9:cbb22d6+, release build, linux [x86_64])',
        },
      },
    });
  });
});
