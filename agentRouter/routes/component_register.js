import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { Codes } from '../../codes.js'

export const path = {
  channel: 'cmd', entity: 'component',
  action: 'register', context: 'component-agent'
}

export const spec = {
  handler: trackProvidedComponentHash,
  post: [
    publishComponentRegistration,
  ],
}

function trackProvidedComponentHash({ message, rootCtx: { connectionRegistry, diagnostics } }) {
  const { connectionId, data: { hash } } = message
  const connection = connectionRegistry.get(connectionId)

  diagnostics.require(
    connection,
    Codes.PRECONDITION_REQUIRED,
    'Connection missing for component registration',
    { connectionId }
  )

  diagnostics.require(
    hash,
    Codes.PRECONDITION_REQUIRED,
    'Component hash is required for registration',
    { connectionId }
  )

  connection.providedComponentHashes.add(hash)

  return { hash }
}

async function publishComponentRegistration({ message, rootCtx: { natsContext } }) {
  const [env, ns, tenant, , , , , version, id] = (message?.subject).split('.')

  const subject = createSubject()
    .set({ env, ns, tenant, version, id })
    .context('gw-ws-components')
    .channel('cmd')
    .entity('component')
    .action('register')
    .build()

  const payload = { data: message.data }
  await natsContext.publish(subject, JSON.stringify(payload))
}
