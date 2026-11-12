import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ui = fileURLToPath(new URL('../../', import.meta.url));
function profile(backend, server, python) {
    const env = { ...process.env, GLAMDB_TEST_BACKEND: backend };
    delete env.GLAMDB_NATIVE_SERVER;
    delete env.NODE_TEST_CONTEXT;
    if (server) env.GLAMDB_NATIVE_SERVER = server;
    if (python) env.GLAMDB_E2E_PYTHON = python;
    return spawnSync(
        process.execPath,
        [
            '--input-type=module',
            '-e',
            'const {default: c} = await import("./playwright.config.mjs"); console.log(JSON.stringify({...c, e2ePython: process.env.GLAMDB_E2E_PYTHON}));',
        ],
        { cwd: ui, env, encoding: 'utf8', timeout: 10000 },
    );
}

test('invalid browser backend fails instead of launching a mock', () => {
    const result = profile('nativ');
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /GLAMDB_TEST_BACKEND/);
});

test('native browser selection requires an executable before running tests', () => {
    for (const executable of [undefined, '/missing/native-server', ui]) {
        const result = profile('native', executable);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /GLAMDB_NATIVE_SERVER/);
    }
});

test('native browser profile includes the shared contract without mock or static startup', () => {
    const result = profile('native', process.execPath);
    assert.equal(result.status, 0, result.stderr);
    const config = JSON.parse(result.stdout);
    assert.deepEqual(config.testMatch, ['native.spec.mjs', 'shared-contract.spec.mjs']);
    assert.equal(config.webServer.length, 1);
    assert.match(config.webServer[0].command, /start-native.py/);
});

test('static demo runs independently without any API backend', () => {
    const result = profile('static');
    assert.equal(result.status, 0, result.stderr);
    const config = JSON.parse(result.stdout);
    assert.deepEqual(config.testMatch, ['public-demo.spec.mjs', 'compact-workbench.spec.mjs']);
    assert.equal(config.webServer.length, 1);
    assert.match(config.webServer[0].command, /start-static.py/);
});

test('browser Python accepts PATH names and relative or absolute executable paths', () => {
    for (const [python, expected] of [
        ['python3', 'python3'],
        [
            '../.venv/bin/python',
            fileURLToPath(new URL('../../../.venv/bin/python', import.meta.url)),
        ],
        ['/usr/bin/python3', '/usr/bin/python3'],
    ]) {
        const result = profile('mock', undefined, python);
        assert.equal(result.status, 0, result.stderr);
        assert.equal(JSON.parse(result.stdout).e2ePython, expected);
    }
});
