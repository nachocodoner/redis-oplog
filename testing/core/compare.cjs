const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const { createHash } = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const revision = 'eaf134973529396b660078b705b9fa50a4747fd5';
const redisPackage = 'cultofcoders:redis-oplog';

function checkCompiledSource(testApp, isopack, source, files) {
    const buildInfo = path.join(testApp, '.meteor/local/isopacks', isopack, 'isopack-buildinfo.json');
    const info = JSON.parse(fs.readFileSync(buildInfo, 'utf8'));
    const watched = new Map();
    for (const build of Object.values(info.unibuildDependencies)) {
        for (const [name, hash] of Object.entries(build.files)) {
            if (hash && fs.existsSync(name)) watched.set(fs.realpathSync(name), hash);
        }
    }
    for (const name of files) {
        const file = fs.realpathSync(path.join(source, name));
        const expected = createHash('sha1').update(fs.readFileSync(file)).digest('hex');
        assert.equal(watched.get(file), expected, 'compiled source for ' + name);
    }
}

async function readResults(browser, port, lane, temp) {
    const page = await browser.newPage();
    const cases = new Map();
    const errors = [];
    const log = path.join(temp, lane + '-browser.log');
    let counted = false;
    page.on('console', message => {
        const line = message.text();
        fs.appendFileSync(log, line + '\n');
        if (line.startsWith('passed/expected/failed/total')) counted = true;
        const result = /^([SC]: .+) : (OK|EXPECTED FAILURE|!+ FAIL !+)$/.exec(line);
        if (result) cases.set(result[1], result[2]);
    });
    page.on('pageerror', error => {
        fs.appendFileSync(log, String(error) + '\n');
        // The pinned reporter sets DONE, then tries to iterate its result object as an array for xUnit.
        const details = String(error);
        const reporterError = counted &&
            details.includes('resultSet.forEach is not a function') &&
            details.includes('/packages/test-in-console.js');
        if (!reporterError) {
            errors.push(details);
        }
    });
    try {
        await page.goto('http://127.0.0.1:' + port);
        await page.waitForFunction(() => window.TEST_STATUS && window.TEST_STATUS.DONE, { timeout: 300000 });
        const result = await page.evaluate(() => window.TEST_STATUS);
        const names = [...cases.keys()].sort();
        const expectedFailures = [...cases.values()].filter(value => value === 'EXPECTED FAILURE');
        assert.equal(names.length, result.PASSED + result.FAILURES + expectedFailures.length);
        const probe = names.find(name => name.endsWith('comparison driver is active'));
        assert.equal(cases.get(probe), 'OK', 'actual observer driver on ' + lane);
        const upstream = names.filter(name => name !== probe);
        const serverCases = upstream.filter(name => name.startsWith('S: '));
        const clientCases = upstream.filter(name => name.startsWith('C: '));
        assert.equal(serverCases.length, 16, 'upstream server cases at the pinned release');
        assert.equal(clientCases.length, 11, 'upstream client cases at the pinned release');
        assert.deepEqual(errors, [], 'browser errors on ' + lane);
        console.log(lane, 'upstream passed', result.PASSED - 1, 'failed', result.FAILURES, 'driver check passed');
        for (const failure of result.WHERE_FAILED) console.log(lane, failure.name);
        return { names: upstream, cases: Object.fromEntries(cases), failures: result.WHERE_FAILED };
    } finally {
        await page.close();
    }
}

module.exports = async function compare({ checkout, temp, app, ports, env, browser, start, startRedis, until, release }) {
    assert.equal(release, 'METEOR@3.6.1', 'use a 3.6.1 test app');
    const archive = execFileSync('git', ['-C', checkout, 'archive', revision, 'packages/mongo']);
    execFileSync('tar', ['-xf', '-', '-C', temp], { input: archive });
    const mongo = path.join(temp, 'packages/mongo');
    const probe = path.join(temp, 'observer-check');
    fs.mkdirSync(probe);
    for (const name of ['package.js', 'driver.js']) {
        fs.copyFileSync(path.join(__dirname, name), path.join(probe, name));
    }
    const results = [];
    for (const lane of ['native', 'redis']) {
        const testApp = path.join(temp, lane + '-app');
        const port = lane === 'native' ? ports.reader : ports.writer;
        const settings = { ...env, OBSERVER_COMPARISON: lane, METEOR_REACTIVITY_ORDER: 'changeStreams,oplog,polling' };
        const args = ['test-packages', '--release', '3.6.1', '--once', '--raw-logs', '--port', '127.0.0.1:' + port,
            '--driver-package', 'test-in-console', '--filter', 'observeChanges -', '--test-app-path', testApp];
        if (lane === 'redis') {
            await startRedis();
            settings.REDIS_OPLOG_SETTINGS = JSON.stringify({ redis: { host: '127.0.0.1', port: ports.redis } });
            settings.METEOR_PACKAGE_DIRS = path.join(app, 'packages');
            args.push('--extra-packages', redisPackage);
        }
        start(lane, 'meteor', [...args, mongo, probe], settings);
        await until(lane, () => fs.readFileSync(path.join(temp, lane + '.log'), 'utf8').includes('test-in-console listening'));
        const versions = fs.readFileSync(path.join(testApp, '.meteor/versions'), 'utf8');
        assert.equal(versions.split('\n').some(entry => entry.startsWith(redisPackage + '@')), lane === 'redis');
        checkCompiledSource(testApp, 'local-test_mongo', mongo, ['tests/observe_changes_tests.js']);
        if (lane === 'redis') {
            checkCompiledSource(testApp, 'cultofcoders_redis-oplog', path.join(app, 'packages/redis-oplog'),
                ['redis-oplog.js', 'lib/mongo/observeChanges.js', 'lib/mongo/ObserveMultiplex.js',
                    'lib/cache/ObservableCollection.js', 'lib/redis/RedisSubscriptionManager.js']);
        }
        results.push(await readResults(browser, port, lane, temp));
    }
    const report = { revision, native: results[0], redis: results[1] };
    fs.writeFileSync(path.join(temp, 'core-results.json'), JSON.stringify(report, null, 2));
    assert.deepEqual(results[1].names, results[0].names, 'identical server and client test inventory');
    assert.deepEqual(results.map(result => result.failures.length), [0, 0], 'upstream failures by lane');
    for (const result of results) {
        assert.deepEqual(result.names.filter(name => result.cases[name] !== 'OK'), [], 'every upstream case passed');
    }
};
