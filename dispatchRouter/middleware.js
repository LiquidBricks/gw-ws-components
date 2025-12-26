import { Codes as Errors } from '../codes.js'

// Decode middleware that extracts data from the message payload.
// Accepts either:
//  - a string key: wraps the entire payload `data` under that key
//  - an array of keys: picks only those keys from payload `data`
// Empty or invalid selectors are not allowed.
export function decodeData(selector) {
  return function ({ message, rootCtx: { diagnostics } }) {
    const { data } = message.json()
    diagnostics.require(
      data,
      Errors.PRECONDITION_REQUIRED,
      'Data is required',
      { field: 'data', subject: message.subject }
    )

    const isString = typeof selector === 'string'
    const isArray = Array.isArray(selector)

    diagnostics.require(
      isString || isArray,
      Errors.PRECONDITION_INVALID,
      'decodeData requires a string key or array of keys',
      { selector }
    )

    if (isString) {
      diagnostics.require(
        selector.length > 0,
        Errors.PRECONDITION_REQUIRED,
        'decodeData key cannot be empty',
        { field: 'selector' }
      )
      // Backward compatible behavior: wrap entire data under the provided key
      return { [selector]: data }
    }

    // Array of keys: pick only those keys from data
    diagnostics.require(
      selector.length > 0,
      Errors.PRECONDITION_REQUIRED,
      'decodeData keys cannot be empty',
      { field: 'selector' }
    )
    diagnostics.require(
      selector.every(k => typeof k === 'string' && k.length > 0),
      Errors.PRECONDITION_INVALID,
      'decodeData keys must be non-empty strings',
      { selector }
    )

    const picked = {}
    for (const k of selector) {
      if (Object.prototype.hasOwnProperty.call(data, k)) picked[k] = data[k]
    }
    return picked
  }
}

// Common post middleware to ack messages
export function ackMessage({ message }) {
  message.ack()
}

// Generic timer middlewares
// Starts a diagnostics timer with a generic code
export function startDiagnosticsTimer({ message, rootCtx: { diagnostics } }) {
  const timer = diagnostics.timer('GENERIC_OPERATION', { subject: message.subject })
  return { timer }
}

// Stops the diagnostics timer; no domain-specific metadata
export function stopDiagnosticsTimer(fn) {
  return function (payload) {
    payload.scope.timer.stop(fn(payload))
  }
}

// No-op receipt middleware: logs receipt, performs no validation/timing
export function acknowledgeReceipt({ message, rootCtx: { diagnostics } }) {
  diagnostics.info('message received (acknowledgeReceipt)', { subject: message.subject })
}
