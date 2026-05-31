import test from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import { WebSocket } from 'ws'

import { gateway } from '../index.js'
import { Codes } from '../codes.js'
import { spec as componentRegisterSpec } from '../agentRouter/routes/component_register.js'
import { spec as computeResultSpec } from '../dispatchRouter/routes/compute_result.js'
import { createRouter as createDispatchRouter } from '../dispatchRouter/index.js'

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
  const iterator = {
    async *[Symbol.asyncIterator]() { /* no messages for tests */ }
  }

  return {
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
          add: async () => { },
        }
      }
    },
  }
}

async function startDispatcher() {
  const server = http.createServer()
  const diagnostics = createDiagnosticsStub()
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const dispatcher = await gateway({
    server,
    path: '/componentAgent',
    streamName: 'COMPONENT_SERVICE_STREAM',
    natsContext: createNatsContextStub(),
    diagnostics,
  })
  const address = server.address()
  const port = typeof address === 'object' && address ? address.port : 0
  const url = `ws://127.0.0.1:${port}/componentAgent`
  return { server, url, diagnostics, dispatcher }
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
      diagnostics.calls.warn.some((args) => args[1] === Codes.PRECONDITION_INVALID),
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
  const { server, url, dispatcher } = await startDispatcher()
  const ws = new WebSocket(url)
  const initialMessage = waitForMessage(ws)

  try {
    await waitForOpen(ws)
    await initialMessage // consume initial connected message

    assert.equal(dispatcher.connectionRegistry.size, 1)
    const [connection] = dispatcher.connectionRegistry.values()
    assert.ok(connection?.providedComponentHashes instanceof Set, 'providedComponentHashes set missing on connection')
    assert.equal(connection.providedComponentHashes.size, 0)

    ws.close()
    await waitForClose(ws)

    assert.equal(dispatcher.connectionRegistry.size, 0)
  } finally {
    await shutdown(ws, server)
  }
})

test('component_register handler records provided hashes per connection and publishes registration', async () => {
  const connectionId = 1
  const connectionRegistry = new Map([
    [connectionId, { publish() { }, providedComponentHashes: new Set() }],
  ])
  const diagnostics = createDiagnosticsStub()
  const message = {
    connectionId,
    data: { hash: 'hash-abc' },
    subject: 'prod.component-service.tenant.component-agent.cmd.component.register.v1.conn-1',
  }

  const scope = await componentRegisterSpec.handler({ message, rootCtx: { connectionRegistry, diagnostics } })
  const entry = connectionRegistry.get(connectionId)
  assert.ok(entry.providedComponentHashes.has('hash-abc'))

  const publishCalls = []
  await componentRegisterSpec.post[0]({
    message,
    scope,
    rootCtx: { natsContext: { publish: async (...args) => publishCalls.push(args) } },
  })

  assert.equal(publishCalls.length, 1)
  const [subject, payload] = publishCalls[0]
  assert.equal(subject, 'prod.component-service.tenant.gw-ws-components.cmd.component.register.v1.conn-1')
  assert.deepEqual(JSON.parse(payload), { data: message.data })
})

test('compute_result publishes via the provider registered for the component hash', async () => {
  const diagnostics = createDiagnosticsStub()
  const publishCalls = []
  const connectionRegistry = new Map([
    [1, { publish: (...args) => publishCalls.push({ connectionId: 1, args }), providedComponentHashes: new Set(['hash-one']) }],
    [2, { publish: (...args) => publishCalls.push({ connectionId: 2, args }), providedComponentHashes: new Set(['hash-two']) }],
  ])
  const scope = {
    instanceId: 'instance-1',
    deps: { foo: 'bar' },
    componentHash: 'hash-two',
    name: 'TestComponent',
    type: 'widget',
  }

  await computeResultSpec.pre[0]({ scope, rootCtx: { diagnostics, connectionRegistry } })

  assert.equal(publishCalls.length, 1)
  const [{ connectionId, args }] = publishCalls
  assert.equal(connectionId, 2)
  const [subject, payload] = args
  assert.equal(subject, 'prod.component-service._._.exec.component.compute_result.v1._')
  assert.deepEqual(payload, {
    componentHash: 'hash-two',
    name: 'TestComponent',
    type: 'widget',
    instanceId: 'instance-1',
    deps: { foo: 'bar' },
  })
})

test('dispatch router does not warn when compute_result has no provider registered', async () => {
  const diagnostics = createDiagnosticsStub()
  const dispatch = createDispatchRouter({
    natsContext: {},
    diagnostics,
    connectionRegistry: new Map(),
  })
  const subject = 'prod.component-service._._.exec.component.compute_result.v1._'
  const requestMessage = {
    subject,
    json() {
      return {
        data: {
          instanceId: 'instance-1',
          deps: {},
          componentHash: 'hash-missing',
          name: 'TestComponent',
          type: 'widget',
        },
      }
    },
  }

  await dispatch.request({ subject, message: requestMessage })

  assert.ok(
    diagnostics.calls.require.some((call) => (
      call.code === Codes.PRECONDITION_REQUIRED &&
      call.message === 'No component provider registered for requested hash'
    ))
  )
  assert.equal(diagnostics.calls.warn.length, 0)
  assert.equal(diagnostics.calls.error.length, 0)
})

test('compute_result validation fails when no provider has the requested hash', () => {
  const diagnostics = createDiagnosticsStub()
  const connectionRegistry = new Map([
    [1, { publish() { }, providedComponentHashes: new Set(['hash-one']) }],
  ])
  const scope = { componentHash: 'hash-missing' }

  assert.throws(
    () => computeResultSpec.pre[0]({ scope, rootCtx: { diagnostics, connectionRegistry } }),
    (err) => err instanceof diagnostics.DiagnosticError && err.code === Codes.PRECONDITION_REQUIRED
  )
})
