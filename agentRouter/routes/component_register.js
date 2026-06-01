import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { Codes } from '../../codes.js'

export const path = {
  channel: 'cmd', entity: 'component',
  action: 'register', context: 'component-agent'
}

export const spec = {
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

async function publishComponentRegistration({ message, rootCtx: { natsContext } }) {
  const [env, ns, tenant, , , , , version] = (message?.subject).split('.')

  const subject = createSubject()
    .set({ env, ns, tenant, version })
    .id(message.agentID)
    .context('gw-ws-components')
    .channel('cmd')
    .entity('componentAgent')
    .action('registerComponent')
    .build()

  const payload = { data: { agentID: message.agentID, component: message.data } }
  await natsContext.publish(subject, JSON.stringify(payload))
}
