'use strict';

const fs = require('node:fs');
const path = require('node:path');

const jobRoot = fs.mkdtempSync(path.join(process.env.RUNNER_TEMP, 'fcp-job-'));
fs.appendFileSync(process.env.GITHUB_STATE, `job_root=${jobRoot}\n`);

let testTemp;
if (process.env.RUNNER_OS === 'Windows') {
  // Keep long fixture paths short without sharing another job's test files.
  const shortParent = 'C:\\fcp-qtmp';
  fs.mkdirSync(shortParent, {recursive: true});
  testTemp = fs.mkdtempSync(path.join(shortParent, 'j-'));
  fs.appendFileSync(process.env.GITHUB_STATE, `short_test_temp=${testTemp}\n`);
} else {
  testTemp = path.join(jobRoot, 'test-tmp');
  fs.mkdirSync(testTemp);
}

fs.appendFileSync(process.env.GITHUB_ENV,
  `FCP_CI_JOB_ROOT=${jobRoot}\nFCP_CI_TEST_TEMP=${testTemp}\n` +
  (process.env.RUNNER_OS === 'Windows'
    ? `TEMP=${testTemp}\nTMP=${testTemp}\n`
    : `TMPDIR=${testTemp}\n`));
