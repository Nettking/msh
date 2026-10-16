'use strict';

const fs = require('node:fs');
const path = require('node:path');

function removeOwnedDirectory(directory, parent, prefix) {
  if (!directory) return;
  const resolved = path.resolve(directory);
  if (path.dirname(resolved) !== path.resolve(parent) ||
      !path.basename(resolved).startsWith(prefix)) {
    throw new Error(`Refusing cleanup outside the job-owned directory: ${resolved}`);
  }
  fs.rmSync(resolved, {recursive: true, force: true, maxRetries: 3});
}

// State is recorded when each unique directory is created, so setup failures
// still clean up. Never enumerate or delete the shared parent directories.
removeOwnedDirectory(process.env.STATE_short_test_temp, 'C:\\fcp-qtmp', 'j-');
removeOwnedDirectory(process.env.STATE_job_root, process.env.RUNNER_TEMP, 'fcp-job-');
