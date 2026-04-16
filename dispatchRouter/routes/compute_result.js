import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { decodeData, ackMessage, acknowledgeReceipt } from '../middleware.js'
import { Codes } from '../../codes.js'

export const path = { channel: 'exec', entity: 'component', action: 'compute_result' }
export const spec = {
  decode: [
    decodeData(['instanceId', 'deps', 'componentHash', 'name', 'type']),
  ],
  pre: [
    findProviderForHash,
  ],
  handler,
  post: [
    publishComputedResult,
    ackMessage,
  ],
}

function handler() {
  // No-op: dispatch happens in the pre stage.
}

function findProviderForHash({
  scope: { componentHash, name, type, instanceId, deps },
  rootCtx: { diagnostics, connectionRegistry },
}) {
  const found = [...connectionRegistry.values()]
    .find(c => c.providedComponentHashes.has(componentHash));

  diagnostics.require(
    found,
    Codes.PRECONDITION_REQUIRED,
    'No component provider registered for requested hash',
    { componentHash },
  )

  const subject = createSubject()
    .env('prod')
    .ns('component-service')
    .entity('component')
    .channel('exec')
    .action('compute_result')
    .version('v1')
    .build()

  found.publish(subject, { componentHash, name, type, instanceId, deps })
  return { publish: found.publish }
}



async function publishComputedResult({ scope, rootCtx: { natsContext, diagnostics } }) {
  // const { instanceId, result, type, name } = scope;
  // const subject = createSubject()
  //   .env('prod')
  //   .ns('component-service')
  //   .entity('componentInstance')
  //   .channel('evt')
  //   .action(`result_computed`)
  //   .version('v1');

  // await natsContext.publish(
  //   subject.build(),
  //   JSON.stringify({ data: { instanceId, name, type, result } })
  // );
}
