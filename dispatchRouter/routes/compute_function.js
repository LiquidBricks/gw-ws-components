import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { decodeData, ackMessage } from '../middleware.js'
import { Codes } from '../../codes.js'

import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'


export const path = createSubject(natsEvents['*'].gateway['*']['*'].cmd.component.compute_function.v1['*'])
  .forSubscribe()
  .toObject()

export const emits = {
  'agent.cmd.component.compute_function.v1':
    natsEvents['*'].agent['*']['*'].cmd.component.compute_function.v1['*'],
}

export const spec = {
  context: { emits },
  decode: [
    decodeData(['instanceId', 'deps', 'componentHash', 'name', 'type']),
  ],
  handler,
  post: [
    ackMessage,
  ],
}

function handler({
  scope: { componentHash, name, type, instanceId, deps },
  rootCtx: { diagnostics, connectionRegistry },
  routeCtx: { emits },
}) {
  const found = [...connectionRegistry.values()]
    .find(c => c.providedComponentHashes.has(componentHash));

  diagnostics.require(
    found,
    Codes.PRECONDITION_REQUIRED,
    'No component provider registered for requested hash',
    { componentHash },
  )

  const subject = createSubject(emits['agent.cmd.component.compute_function.v1']).forPublish()
    .env('prod')
    .build()

  found.publish(
    subject,
    { componentHash, name, type, instanceId, deps },
    { headers: {} }
  )
}
