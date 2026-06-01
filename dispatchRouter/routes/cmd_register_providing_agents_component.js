import { decodeData } from '../middleware.js'
import { Codes } from '../../codes.js'

export const path = {
  channel: 'exec',
  entity: 'componentAgent',
  action: 'cmdRegisterProvidingAgentsComponent',
}

export const spec = {
  decode: [
    decodeData(['agentID', 'hash']),
  ],
  handler: registerProvidingAgentsComponent,
}

function registerProvidingAgentsComponent({
  scope: { agentID, hash },
  rootCtx: { diagnostics, connectionRegistry },
}) {
  diagnostics.require(
    typeof agentID === 'string' && agentID.length,
    Codes.PRECONDITION_REQUIRED,
    'agentID is required for provider registration',
    { field: 'agentID' },
  )
  diagnostics.require(
    typeof hash === 'string' && hash.length,
    Codes.PRECONDITION_REQUIRED,
    'Component hash is required for provider registration',
    { field: 'hash', agentID },
  )

  const connection = connectionRegistry.get(agentID)
  diagnostics.require(
    connection,
    Codes.PRECONDITION_REQUIRED,
    'Connection missing for provider registration',
    { agentID, hash },
  )

  connection.providedComponentHashes.add(hash)
  return { agentID, hash }
}
