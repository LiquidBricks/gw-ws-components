import router from "@liquid-bricks/lib-nats-subject/router";
import { path as computeResultPath, spec as computeResultSpec } from './routes/compute_result.js'
import { path as cmdRegisterProvidingAgentsComponentPath, spec as cmdRegisterProvidingAgentsComponentSpec } from './routes/cmd_register_providing_agents_component.js'
import { Codes } from '../codes.js'

export function createRouter({
  natsContext,
  diagnostics,
  connectionRegistry,
}) {
  return router({
    tokens: ['env', 'ns', 'tenant', 'context', 'channel', 'entity', 'action', 'version', 'id'],
    context: { natsContext, diagnostics, connectionRegistry },
  })
    .route(computeResultPath, computeResultSpec)
    .route(cmdRegisterProvidingAgentsComponentPath, cmdRegisterProvidingAgentsComponentSpec)
    .default({
      handler: async ({ message, rootCtx: { diagnostics } }) => {
        diagnostics.invariant(
          message.term(`No handler for subject: ${message.subject}`) ?? false,
          Codes.ROUTER_UNKNOWN_SUBJECT,
          `No handler for subject: ${message.subject}`,
          { subject: message.subject, message: message?.json?.() }
        )
      }
    })
    .error(({ error, rootCtx: { diagnostics } }, ...rest) => {
      if (error instanceof diagnostics.DiagnosticError) {
        return
      }
      throw diagnostics.error(
        Codes.ROUTER_HANDLER_ERROR,
        'gw-ws-components router error',
        { error, rest },
      )
    })
    .abort(({ reason, stage, message, rootCtx: { diagnostics } }) => {
      try { message?.ack?.() } catch (_) { /* ignore */ }
      return { status: 'aborted' }
    })
}
