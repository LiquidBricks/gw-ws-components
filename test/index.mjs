import test from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { WebSocket } from 'ws'

import { gateway } from '../index.js'
import { PRECONDITION_INVALID } from '@liquid-bricks/lib-diagnostics/codes'

const DEFAULT_TIMEOUT = 5000

function createDiagnosticsStub() {
  const calls = { info: [], warn: [], require: [], debug: [], invariant: [], error: [] }
  class DiagnosticError extends Error {
    constructor(message, code) {
      super(message)
      this.code = code
    }
  }
  const stub = {
    calls,
    DiagnosticError,
    child() { return stub },
    info(...args) { calls.info.push(args) },
    warn(...args) { calls.warn.push(args) },
    debug(...args) { calls.debug.push(args) },
    invariant(value, code, message, meta) {
      calls.invariant.push({ value, code, message, meta })
      if (!value) throw new DiagnosticError(message, code)
    },
    error(code, message, meta) {
      calls.error.push({ code, message, meta })
      return new DiagnosticError(message, code)
    },
    require(value, code, message, meta) {
      calls.require.push({ value, code, message, meta })
      if (!value) throw new DiagnosticError(message, code)
    },
  }
  return stub
}

function createNatsContextStub() {
  const publishCalls = []
  const consumerAddCalls = []
  const iterator = {
    async *[Symbol.asyncIterator]() { /* no messages for tests */ }
  }

  return {
    publishCalls,
    consumerAddCalls,
    publish: async (...args) => publishCalls.push(args),
    async jetstream() {
      return {
        consumers: {
          get: async () => ({ consume: () => iterator }),
        }
      }
    },
    async jetstreamManager() {
      return {
        consumers: {
          delete: async () => { },
          add: async (...args) => consumerAddCalls.push(args),
        }
      }
    },
  }
}

async function startDispatcher() {
  const server = http.createServer()
  const diagnostics = createDiagnosticsStub()
  const natsContext = createNatsContextStub()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const dispatcher = await gateway({
    server,
    path: '/componentAgent',
    streamName: 'COMPONENT_SERVICE_STREAM',
    natsContext,
    diagnostics,
  })
  const address = server.address()
  const port = typeof address === 'object' && address ? address.port : 0
  const url = `ws://127.0.0.1:${port}/componentAgent`
  return { server, url, diagnostics, dispatcher, natsContext }
}

async function closeServer(server) {
  await new Promise((resolve) => server.close(resolve))
}

async function shutdown(ws, server) {
  try {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      ws.close()
      try { await waitForClose(ws) } catch { }
    }
  } finally {
    try { await closeServer(server) } catch { }
  }
}

function waitForOpen(ws, timeoutMs = DEFAULT_TIMEOUT) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket open timed out')), timeoutMs)
    ws.once('open', () => { clearTimeout(timer); resolve() })
    ws.once('error', (err) => { clearTimeout(timer); reject(err) })
  })
}

function waitForMessage(ws, timeoutMs = DEFAULT_TIMEOUT) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket message timed out')), timeoutMs)
    ws.once('message', (data) => {
      clearTimeout(timer)
      resolve(typeof data === 'string' ? data : data.toString())
    })
    ws.once('error', (err) => { clearTimeout(timer); reject(err) })
    ws.once('close', () => { clearTimeout(timer); reject(new Error('WebSocket closed before message')) })
  })
}

function waitForClose(ws, timeoutMs = DEFAULT_TIMEOUT) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('WebSocket close timed out')), timeoutMs)
    ws.once('close', () => { clearTimeout(timer); resolve() })
    ws.once('error', (err) => { clearTimeout(timer); reject(err) })
  })
}

test('gateway subscribes to compute_function commands', async () => {
  const { server, natsContext } = await startDispatcher()

  try {
    assert.equal(natsContext.consumerAddCalls.length, 1)
    const [, config] = natsContext.consumerAddCalls[0]
    assert.equal(config.filter_subjects[0], 'prod.gateway.*.*.cmd.component.compute_function.v1.>')
  } finally {
    await closeServer(server)
  }
})

test('gateway sends an initial connected message', async () => {
  const { server, url } = await startDispatcher()
  const ws = new WebSocket(url)
  const initialMessage = waitForMessage(ws)

  try {
    await waitForOpen(ws)
    const initial = JSON.parse(await initialMessage)
    assert.deepEqual(initial, { ok: true, message: 'gw-ws-components connected' })
  } finally {
    await shutdown(ws, server)
  }
})

test('gateway rejects invalid JSON payloads', async () => {
  const { server, url, diagnostics } = await startDispatcher()
  const ws = new WebSocket(url)
  const initialMessage = waitForMessage(ws)

  try {
    await waitForOpen(ws)
    await initialMessage // consume initial connected message

    ws.send('not-json')
    const response = JSON.parse(await waitForMessage(ws))

    assert.equal(response.ok, false)
    assert.equal(response.error, 'Invalid JSON payload')
    assert.ok(
      diagnostics.calls.warn.some((args) => args[1] === PRECONDITION_INVALID),
      'warn called with PRECONDITION_INVALID'
    )
  } finally {
    await shutdown(ws, server)
  }
})

test('gateway replies with generic not-served response for valid payloads', async () => {
  const { server, url } = await startDispatcher()
  const ws = new WebSocket(url)
  const initialMessage = waitForMessage(ws)

  try {
    await waitForOpen(ws)
    await initialMessage // consume initial connected message

    ws.send(JSON.stringify({ ping: true }))
    const response = JSON.parse(await waitForMessage(ws))

    assert.deepEqual(response, { ok: false, error: 'gw-ws-components does not serve components' })
  } finally {
    await shutdown(ws, server)
  }
})

test('gateway tracks connections in a registry', async () => {
  const { server, url, dispatcher, natsContext } = await startDispatcher()
  const ws = new WebSocket(url)
  const initialMessage = waitForMessage(ws)

  try {
    await waitForOpen(ws)
    await initialMessage // consume initial connected message

    assert.equal(dispatcher.connectionRegistry.size, 1)
    const [[agentID, connection]] = dispatcher.connectionRegistry.entries()
    assert.equal(agentID.length, 36)
    assert.equal(connection.agentID, agentID)
    assert.ok(connection.providedComponentHashes instanceof Set, 'providedComponentHashes set missing on connection')
    assert.equal(connection.providedComponentHashes.size, 0)
    assert.equal(natsContext.publishCalls.length, 1)
    const [subject, payload] = natsContext.publishCalls[0]
    assert.equal(subject, 'prod.component-service._.gw-ws-components.cmd.componentAgent.register.v1.' + agentID)
    assert.deepEqual(JSON.parse(payload), { data: { agentID } })

    ws.close()
    await waitForClose(ws)

    assert.equal(dispatcher.connectionRegistry.size, 0)
  } finally {
    await shutdown(ws, server)
  }
})
