// From the CI-style bare Meteor test app, run meteor node ../testing/cross-instance/run.cjs.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { randomInt } = require('node:crypto');
const { execFileSync, spawn } = require('node:child_process');
const { setTimeout: delay } = require('node:timers/promises');
const { createRequire } = require('node:module');
const exercise = require('./exercise.cjs');
const boundaries = require('./boundaries.cjs');

const template = process.cwd();
const root = path.resolve(__dirname, '../..');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'redis-oplog-observers-'));
const app = path.join(temp, 'app');
const children = [];
let browser;

async function availablePorts(count = 4) {
    for (let attempt = 0; attempt < 20; attempt++) {
        const base = randomInt(20000, 50000);
        const sockets = [];
        try {
            for (let offset = 0; offset < count; offset++) {
                const server = net.createServer();
                sockets.push(server);
                await new Promise((resolve, reject) => {
                    server.once('error', reject).listen(base + offset, '127.0.0.1', resolve);
                });
            }
            return { reader: base, writer: base + 2, redis: base + count - 1 };
        } catch (error) {
            if (error.code !== 'EADDRINUSE') throw error;
        } finally {
            for (const socket of sockets) if (socket.listening) socket.close();
        }
    }
    throw new Error('Could not find consecutive free loopback ports');
}

function start(label, executable, args, env) {
    const log = fs.openSync(path.join(temp, label + '.log'), 'w');
    try {
        const child = spawn(executable, args, {
            cwd: app, env, stdio: ['ignore', log, log], detached: true,
        });
        child.on('error', error => { child.startError = error; });
        children.push({ child, label });
    } finally {
        fs.closeSync(log);
    }
}

async function until(label, ready) {
    const deadline = Date.now() + 180000;
    while (Date.now() < deadline) {
        for (const { child, label: service } of children) {
            if (child.startError) throw child.startError;
            if (child.exitCode !== null || child.signalCode) throw new Error(service + ' exited');
        }
        if (ready()) return;
        await delay(100);
    }
    throw new Error('Timed out starting ' + label);
}

function signalGroups(signal) {
    for (const { child } of children.slice().reverse()) {
        if (!child.pid) continue;
        try { process.kill(-child.pid, signal); }
        catch (error) { if (error.code !== 'ESRCH') throw error; }
    }
}

async function shutDown() {
    try { if (browser) await browser.close(); }
    finally {
        signalGroups('SIGTERM');
        await delay(500);
        signalGroups('SIGKILL');
    }
}

function copyLocalPackage(localPackage) {
    fs.mkdirSync(localPackage, { recursive: true });
    for (const name of ['package.js', 'README.md', 'redis-oplog.js', 'redis-oplog.client.js', 'lib', '.npm/package/npm-shrinkwrap.json']) {
        fs.cpSync(path.join(root, name), path.join(localPackage, name), { recursive: true });
    }
}

async function main() {
    const args = process.argv.slice(2);
    const core = args[0] === '--core-parity' && args.length === 2;
    const modes = ['--reload-control', '--reload-overlap', '--initial-handoff', '--pre-resubscribe'];
    if (args.length && !core && (args.length !== 1 || !modes.includes(args[0]))) {
        throw new Error('Expected one of ' + modes.join(', ') + ' or --core-parity <meteor-checkout>');
    }
    const puppeteer = createRequire(path.join(template, 'package.json'))('puppeteer');
    const release = fs.readFileSync(path.join(template, '.meteor/release'), 'utf8');
    const ports = await availablePorts(core ? 5 : 4);
    fs.mkdirSync(path.join(app, '.meteor'), { recursive: true });
    fs.writeFileSync(path.join(app, '.meteor/release'), release);
    fs.writeFileSync(path.join(app, '.meteor/platforms'), 'server\nbrowser\n');
    fs.writeFileSync(path.join(app, '.meteor/packages'), 'meteor-base\nmongo\necmascript\ncultofcoders:redis-oplog\n');
    for (const name of ['package.json', 'package-lock.json']) {
        fs.copyFileSync(path.join(template, name), path.join(app, name));
    }

    const localPackage = path.join(app, 'packages/redis-oplog');
    copyLocalPackage(localPackage);
    for (const side of ['client', 'server']) {
        fs.mkdirSync(path.join(app, side));
        fs.copyFileSync(path.join(__dirname, side + '.js'), path.join(app, side, 'main.js'));
    }

    const env = { ...process.env };
    for (const name of ['MONGO_URL', 'MONGO_OPLOG_URL', 'REDIS_OPLOG_SETTINGS', 'METEOR_SETTINGS',
        'METEOR_PACKAGE_DIRS', 'METEOR_LOCAL_DIR', 'PACKAGE_DIRS', 'ROOT_URL', 'PORT', 'DDP_DEFAULT_CONNECTION_URL']) {
        delete env[name];
    }
    Object.assign(env, {
        BIND_IP: '127.0.0.1', TEST_REDIS_PORT: String(ports.redis),
        METEOR_NO_RELEASE_CHECK: '1', DO_NOT_TRACK: '1',
    });
    async function startRedis(socketOptions = []) {
        start('redis-broker', 'redis-server', ['--bind', '127.0.0.1', '--port', String(ports.redis),
            ...socketOptions, '--dir', temp, '--save', '', '--appendonly', 'no'], env);
        const readLog = () => fs.readFileSync(path.join(temp, 'redis-broker.log'), 'utf8');
        await until('Redis', () => readLog().includes('Ready to accept connections'));
        const version = readLog().match(/Redis version=([^,\s]+)/);
        if (version) console.log('Redis server', version[1], 'on port', ports.redis);
    }
    if (core) {
        browser = await puppeteer.launch();
        const compare = require('../core/compare.cjs');
        await compare({ checkout: path.resolve(args[1]), temp, app, ports, env, browser, start, startRedis, until, release: release.trim() });
        return;
    }
    execFileSync('meteor', ['npm', 'ci', '--no-audit', '--no-fund'], {
        cwd: app, env: { ...env, PUPPETEER_SKIP_CHROMIUM_DOWNLOAD: 'true' }, timeout: 180000,
    });
    const readerSocket = path.join(temp, 'redis.sock');
    await startRedis(['--unixsocket', readerSocket, '--unixsocketperm', '700']);

    const readerReady = path.join(temp, 'reader.ready');
    const writerReady = path.join(temp, 'writer.ready');
    const url = port => 'http://127.0.0.1:' + port;
    start('reader', 'meteor', ['run', '--once', '--port', '127.0.0.1:' + ports.reader], {
        ...env, TEST_READY_FILE: readerReady, TEST_ROLE: 'reader', TEST_READER_SOCKET: readerSocket,
    });
    await until('reader', () => fs.existsSync(readerReady));
    const mongoUrl = fs.readFileSync(readerReady, 'utf8');
    const mongo = new URL(mongoUrl);
    if (mongo.hostname !== '127.0.0.1' || Number(mongo.port) !== ports.reader + 1) {
        throw new Error('Expected the reader to start its own local MongoDB');
    }
    start('writer', process.execPath, [path.join(app, '.meteor/local/build/main.js')], {
        ...env, TEST_READY_FILE: writerReady, TEST_ROLE: 'writer',
        MONGO_URL: mongoUrl, ROOT_URL: url(ports.writer), PORT: String(ports.writer),
    });
    await until('writer', () => fs.existsSync(writerReady));
    browser = await puppeteer.launch();
    const mode = args[0];
    async function runScenarios() {
        if (!mode || mode === '--reload-control' || mode === '--reload-overlap') {
            await exercise(browser, url(ports.reader), url(ports.writer), mode !== '--reload-control');
        }
        if (!mode || mode === '--initial-handoff' || mode === '--pre-resubscribe') {
            await boundaries(browser, url(ports.reader), url(ports.writer), mode);
        }
    }
    let timer;
    try {
        await Promise.race([
            runScenarios(),
            new Promise((_, reject) => {
                timer = setTimeout(() => reject(new Error('Cross-instance exercise timed out')), 90000);
            }),
        ]);
    } finally {
        clearTimeout(timer);
    }
}

for (const signal of ['SIGTERM', 'SIGINT']) {
    process.once(signal, async () => {
        await shutDown();
        process.exit(signal === 'SIGINT' ? 130 : 143);
    });
}

main().then(() => {
    console.log('Passed cross-instance browser scenarios', process.argv[2] || '(default)');
}).catch(error => {
    process.exitCode = 1;
    console.error(error);
    console.error('Local fixture and logs:', temp);
}).finally(async () => {
    await shutDown();
    if (!process.exitCode) fs.rmSync(temp, { recursive: true, force: true });
});
