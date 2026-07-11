import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'
import { Codes } from '../../codes.js'


export const path = createSubject(natsEvents['*'].component_service['*']['*'].cmd.component.register.v1['*'])
  .forSubscribe()
  .context('component-agent')
  .toObject()

export const emits = {
  'component_service.cmd.componentAgent.registerComponent.v1':
    natsEvents['*'].component_service['*']['*'].cmd.componentAgent.registerComponent.v1['*'],
}

export const spec = {
  context: { emits },
  handler: validateComponentRegistration,
  post: [
    publishComponentRegistration,
  ],
}

function validateComponentRegistration({ message, rootCtx: { connectionRegistry, diagnostics } }) {
  const { agentID, data: { hash } } = message
  const connection = connectionRegistry.get(agentID)

  diagnostics.require(
    connection,
    Codes.PRECONDITION_REQUIRED,
    'Connection missing for component registration',
    { agentID }
  )

  diagnostics.require(
    hash,
    Codes.PRECONDITION_REQUIRED,
    'Component hash is required for registration',
    { agentID }
  )

  return { hash, agentID }
}

async function publishComponentRegistration({ message, rootCtx: { natsContext }, routeCtx: { emits } }) {
  const [env, , tenant] = (message?.subject).split('.')

  const subject = createSubject(emits['component_service.cmd.componentAgent.registerComponent.v1']).forPublish()
    .set({ env, tenant })
    .context('gw-ws-components')
    .id(message.agentID)
    .build()

  const payload = { data: { agentID: message.agentID, component: message.data } }
  await natsContext.publish(subject, JSON.stringify(payload))
}
