#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const path = require("node:path");

const python = process.env.CHANGI_PYTHON || "python3";
const script = path.join(__dirname, "..", "changid");
const result = spawnSync(python, [script, ...process.argv.slice(2)], {
  stdio: "inherit",
});

if (result.error) {
  console.error(`changid: unable to start ${python}: ${result.error.message}`);
  console.error("changid requires Python 3.10 or newer on Linux.");
  process.exit(127);
}

if (result.signal) {
  const signalNumber = require("node:os").constants.signals[result.signal];
  process.exit(128 + (signalNumber || 1));
}

process.exit(result.status ?? 1);
