import { AckPolicy, DeliverPolicy } from "@nats-io/jetstream";
import { WebSocketServer } from 'ws';
import { Codes } from './codes.js';
import { createRouter as dispatchRouter } from './dispatchRouter/index.js';
import { createRouter as agentRouter } from './agentRouter/index.js';

const consumerName = 'gwWsComponentsConsumer'

export async function gateway({
  server,
  streamName,
  natsContext,
  diagnostics: d,
}) {
  const diagnostics = d.child({ route: 'gw-ws-components' })
  const wss = new WebSocketServer({ server, path: '/componentAgent' });
  const connectionRegistry = new Map();

  diagnostics.require(wss, Codes.PRECONDITION_REQUIRED, 'wss is required', { field: 'wss' });

  const iter = await startConsumer({ streamName, natsContext, diagnostics })
    .catch((error) => diagnostics.warn(false, Codes.PRECONDITION_INVALID, 'gw-ws-components consumer failed to start', { error: error?.message ?? String(error) }))

  const r = dispatchRouter({ natsContext, diagnostics: diagnostics.child({ direction: 'dispatch' }), connectionRegistry })
  const a = agentRouter({ natsContext, diagnostics: diagnostics.child({ direction: 'agent' }), connectionRegistry })

  if (iter?.[Symbol.asyncIterator]) {
    new Promise(async () => {
      for await (const m of iter) {
        await r.request({ subject: m.subject, message: m })
        m.ack()
      }
    })
  }

  let connectionCounter = 0;
  wss.on('connection', (ws, req) => {
    const connectionId = ++connectionCounter;
    const connectionDiagnostics = diagnostics.child({ connectionId });
    connectionRegistry.set(connectionId, {
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
        parsed = { ...JSON.parse(raw), connectionId };
      } catch (error) {
        connectionDiagnostics.warn(false, Codes.PRECONDITION_INVALID,
          'gw-ws-components received invalid JSON', {
          raw,
          error: error?.message ?? String(error),
        });
        ws.send(JSON.stringify({ ok: false, error: 'Invalid JSON payload' }));
        return;
      }

      const { subject } = parsed;

      if (typeof subject !== 'string' || subject.length === 0) {
        connectionDiagnostics.warn(false, Codes.PRECONDITION_INVALID, 'gw-ws-components received message without subject', { subject });
        ws.send(JSON.stringify({ ok: false, error: 'gw-ws-components does not serve components' }));
        return;
      }

      await a.request({ subject, message: parsed })

    });

    ws.on('close', () => {
      connectionRegistry.delete(connectionId);
      connectionDiagnostics.info('componentAgent disconnected');
    });

    ws.on('error', (err) => connectionDiagnostics.warn(
      false,
      Codes.PRECONDITION_INVALID,
      'gw-ws-components socket error',
      { error: err?.message ?? String(err) },
    ));

    ws.send(JSON.stringify({ ok: true, message: 'gw-ws-components connected' }));
  });

  return { wss, connectionRegistry };
}

async function startConsumer({ streamName, natsContext, diagnostics }) {
  diagnostics.require(streamName, Codes.PRECONDITION_REQUIRED, 'streamName is required', { field: 'streamName' });
  diagnostics.require(natsContext, Codes.PRECONDITION_REQUIRED, 'connection is required', { field: 'natsContext' });

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
      'prod.component-service.*.*.exec.component.compute_result.v1.>',
    ]
  });
  const c = await jetstream.consumers.get(streamName, consumerName);
  return c.consume();

}
