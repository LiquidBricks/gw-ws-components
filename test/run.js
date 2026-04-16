#!/usr/bin/env node
import { run } from 'node:test'
import { spec } from 'node:test/reporters'
import path from 'node:path'
import fs from 'node:fs'

// Store artifacts under test/output relative to this file
const outputDir = path.resolve(import.meta.dirname, 'output')
fs.mkdirSync(outputDir, { recursive: true })

const now = Date.now()
// Reverse-lexicographically sortable prefix: smaller for newer timestamps
const MAX_SAFE = Number.MAX_SAFE_INTEGER
const reverseKey = (BigInt(MAX_SAFE) - BigInt(now))
  .toString()
  .padStart(String(MAX_SAFE).length, '0')

// Human-readable timestamp alongside the reverse key
const timestamp = new Date(now).toISOString().replace(/[:.]/g, '-')
const outputFile = path.resolve(outputDir, `${reverseKey}__results-${timestamp}.txt`)
const fileStream = fs.createWriteStream(outputFile)

// Discover tests under test/**/*.mjs (excluding this runner which is .js)
const stream = run({
  concurrency: true,
  globPatterns: [
    path.resolve(import.meta.dirname, '**/*.mjs')
  ]
})
  .on('test:fail', () => {
    process.exitCode = 1
  })
  .compose(spec)

stream.pipe(process.stdout)
stream.pipe(fileStream)

// Ensure the process exits after tests complete and the file is flushed
stream.once('end', () => {
  if (!fileStream.closed) fileStream.end()
})
fileStream.once('finish', () => {
  process.exit(process.exitCode ?? 0)
})
