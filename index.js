import { create as createSubject } from '@liquid-bricks/lib-nats-subject/create/basic';
import { randomUUID } from 'node:crypto';
import { AckPolicy, DeliverPolicy } from "@nats-io/jetstream";
import { WebSocketServer } from 'ws';
import {
  PRECONDITION_INVALID,
  PRECONDITION_REQUIRED,
} from '@liquid-bricks/lib-diagnostics/codes';
import { createNatsIngressRouter } from '@liquid-bricks/gw-ws-components-nats-to-ws';
import { createWebSocketIngressRouter } from '@liquid-bricks/gw-ws-components-ws-to-nats';

import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'


const consumerName = 'gwWsComponentsConsumer'

export async function gateway({
  server,
  path,
  streamName,
  natsContext,
  diagnostics: d,
}) {
  const diagnostics = d.child({ route: 'gw-ws-components' })
  diagnostics.require(path, PRECONDITION_REQUIRED, 'path is required', { field: 'path' });
  const wss = new WebSocketServer({ server, path });
  const connectionRegistry = new Map();

  diagnostics.require(wss, PRECONDITION_REQUIRED, 'wss is required', { field: 'wss' });

  const iter = await startConsumer({ streamName, natsContext, diagnostics })
    .catch((error) => diagnostics.warn(false, PRECONDITION_INVALID, 'gw-ws-components consumer failed to start', { error: error?.message ?? String(error) }))

  const natsIngressRouter = createNatsIngressRouter({
    natsContext,
    diagnostics: diagnostics.child({ direction: 'nats-ingress' }),
    connectionRegistry,
  })
  const webSocketIngressRouter = createWebSocketIngressRouter({
    natsContext,
    diagnostics: diagnostics.child({ direction: 'websocket-ingress' }),
    connectionRegistry,
  })

  if (iter?.[Symbol.asyncIterator]) {
    new Promise(async () => {
      for await (const m of iter) {
        await natsIngressRouter.request({ subject: m.subject, message: m })
      }
    })
  }

  wss.on('connection', async (ws, req) => {
    const agentID = randomUUID();
    const connectionDiagnostics = diagnostics.child({ agentID });
    connectionRegistry.set(agentID, {
      agentID,
      publish: (subject, data) => {
        const payload = JSON.stringify({ subject, data });
        ws.send(payload);
      },
      providedComponentHashes: new Set(),
    });
    connectionDiagnostics.info('gw-ws-components connected', { remoteAddress: req?.socket?.remoteAddress });

    ws.on('message', async (raw) => {
      let parsed;

      try {
        parsed = { ...JSON.parse(raw), agentID };
      } catch (error) {
        connectionDiagnostics.warn(false, PRECONDITION_INVALID,
          'gw-ws-components received invalid JSON', {
          raw,
          error: error?.message ?? String(error),
        });
        ws.send(JSON.stringify({ ok: false, error: 'Invalid JSON payload' }));
        return;
      }

      const { subject } = parsed;

      if (typeof subject !== 'string' || subject.length === 0) {
        connectionDiagnostics.warn(false, PRECONDITION_INVALID, 'gw-ws-components received message without subject', { subject });
        ws.send(JSON.stringify({ ok: false, error: 'gw-ws-components does not serve components' }));
        return;
      }

      await webSocketIngressRouter.request({ subject, message: parsed })

    });

    ws.on('close', () => {
      connectionRegistry.delete(agentID);
      connectionDiagnostics.info('componentAgent disconnected');
    });

    ws.on('error', (err) => connectionDiagnostics.warn(
      false,
      PRECONDITION_INVALID,
      'gw-ws-components socket error',
      { error: err?.message ?? String(err) },
    ));

    try {
      await publishComponentAgentRegistration({ natsContext, agentID });
    } catch (error) {
      connectionDiagnostics.warn(false, PRECONDITION_INVALID, 'componentAgent registration publish failed', { error: error?.message ?? String(error) });
    }

    ws.send(JSON.stringify({ ok: true, message: 'gw-ws-components connected' }));
  });

  return { wss, connectionRegistry };
}

async function publishComponentAgentRegistration({ natsContext, agentID }) {
  const subject = createSubject(natsEvents['*'].component_service['*']['*'].cmd.componentAgent.register.v1['*']).forPublish()
    .env('prod')
    .context('gw-ws-components')
    .id(agentID)
    .build();

  await natsContext.publish(subject, JSON.stringify({ data: { agentID } }));
}

async function startConsumer({ streamName, natsContext, diagnostics }) {
  diagnostics.require(streamName, PRECONDITION_REQUIRED, 'streamName is required', { field: 'streamName' });
  diagnostics.require(natsContext, PRECONDITION_REQUIRED, 'connection is required', { field: 'natsContext' });

  const jetstream = await natsContext.jetstream();
  const jetstreamManager = await natsContext.jetstreamManager()

  try {
    await jetstreamManager.consumers.delete(streamName, consumerName)
  } catch (err) {
    // ignore if consumer does not exist or deletion fails non-fatally
  }

  await jetstreamManager.consumers.add(streamName, {
    durable_name: consumerName,
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    filter_subjects: [
      createSubject(natsEvents['*'].gateway['*']['*'].cmd.component.compute_function.v1['*']).forSubscribe().env('prod').id('>').build(),
      createSubject(natsEvents['*'].component_service['*']['*'].exec.componentAgent.cmdRegisterProvidingAgentsComponent.v1['*']).forSubscribe().env('prod').id('>').build(),
    ]
  });
  const c = await jetstream.consumers.get(streamName, consumerName);
  return c.consume();

}
