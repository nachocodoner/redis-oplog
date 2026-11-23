const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');
const waitFor = require('./wait-for.cjs');

module.exports = async function boundaries(browser, readerUrl, writerUrl, mode) {
    // Subscription setup can block its DDP connection. Drive the gates from a separate connection.
    const [admin, client, joining] = await Promise.all([browser.newPage(), browser.newPage(), browser.newPage()]);
    const reader = (name, ...args) => admin.evaluate((name, args) => crossInstance.callReader(name, ...args), name, args);
    const writer = (name, ...args) => client.evaluate((name, args) => window.peer.callAsync(name, ...args), name, args);
    const counts = () => reader('boundaryChannels');
    const rows = (page = client) => page.evaluate(() => crossInstance.items.find({}, { sort: { _id: 1 } }).fetch());
    const publish = (group, id, operation) => writer('writeAndNotify', group, id, operation || 'insert');
    const subscribe = (group, page = client) => page.evaluate(group => {
        window.subscriptionError = null;
        window.boundarySub = crossInstance.subscribe('boundaryItems', group, {
            onStop(error) { if (error) window.subscriptionError = error.error; },
        });
    }, group);
    const stop = (page = client) => page.evaluate(() => { if (window.boundarySub) boundarySub.stop(); });
    const expected = ids => ids.slice().sort().map(_id => ({ _id, value: 1 }));

    async function checkAfterMarker(group) {
        const id = group + '-marker';
        assert.equal(await publish(group, id), 1, 'live marker recipients');
        await client.waitForFunction(id => !!crossInstance.items.findOne(id), {}, id);
        const mongo = await writer('boundaryRows', group);
        assert.deepEqual(mongo, expected([group + '-seed', group + '-gap', id]));
        await waitFor(group + ' browser view after the live marker', rows, mongo);
    }

    async function initialHandoff(inside) {
        const group = inside ? 'initial' : 'initial-control';
        const removed = group + '-gone';
        assert.equal(await publish(group, group + '-seed'), 0);
        assert.equal(await publish(group, removed), 0);
        async function changeMembership() {
            return [await publish(group, group + '-gap'), await publish(group, removed, 'remove')];
        }
        await reader('holdInitialRead', group);
        try {
            await subscribe(group);
            await waitFor('initial Mongo snapshot captured', () => reader('initialReadIds'), [removed, group + '-seed']);
            if (inside) {
                await subscribe(group, joining);
                await waitFor('second publication started', () => reader('initialPublications'), 2);
                const recipients = await changeMembership();
                console.log('Initial insert/remove PUBLISH recipients', recipients);
                if (recipients.every(count => count === 1)) {
                    await waitFor('reader received both writes during the initial query', () => reader('initialMessages'),
                        [['i', group + '-gap'], ['r', removed]]);
                }
            }
            await reader('releaseInitialRead');
            await client.waitForFunction(() => boundarySub.ready());
            if (inside) {
                await joining.waitForFunction(() => boundarySub.ready());
                const mongo = await writer('boundaryRows', group);
                for (const page of [client, joining]) {
                    assert.deepEqual(await rows(page), mongo, 'projected initial membership at ready');
                }
            }
            await waitFor('initial channel attached', counts, [1, 0]);
            if (!inside) assert.deepEqual(await changeMembership(), [1, 1]);
            await checkAfterMarker(group);
            if (inside) {
                await waitFor('joining browser view', () => rows(joining), await writer('boundaryRows', group));
            }
        } finally {
            await reader('releaseInitialRead');
            await stop();
            if (inside) await stop(joining);
            await waitFor('initial observer released', counts, [0, 0]);
        }
    }

    async function failedInitialRead() {
        const group = 'initial-retry';
        await publish(group, 'retry-seed');
        await reader('holdInitialRead', group, true);
        try {
            await subscribe(group);
            await waitFor('failed query reached gate', () => reader('initialReadIds'), ['retry-seed']);
            await subscribe(group, joining);
            await waitFor('both handles joined the failing query', () => reader('initialPublications'), 2);
            await reader('releaseInitialRead');
            for (const page of [client, joining]) {
                await waitFor('initial error reaches each subscriber', () => page.evaluate(() => subscriptionError), 'test-initial-read');
            }
            await waitFor('failed observer unsubscribed', counts, [0, 0]);
            await subscribe(group);
            await client.waitForFunction(() => boundarySub.ready());
            await waitFor('retry receives fresh snapshot', rows, expected(['retry-seed']));
        } finally {
            await reader('releaseInitialRead');
            await stop();
            await stop(joining);
            await waitFor('retry released its channel', counts, [0, 0]);
        }
    }

    async function reconnectDuringInitialRead() {
        const group = 'initial-reconnect';
        await publish(group, group + '-seed');
        await reader('holdInitialRead', group);
        try {
            await subscribe(group);
            await waitFor('startup read before disconnect', () => reader('initialReadIds'), [group + '-seed']);
            await waitFor('initial channel acknowledged', counts, [1, 0]);
            await reader('disconnectListener');
            try {
                await waitFor('startup listener offline', counts, [0, 0]);
                assert.equal(await publish(group, group + '-gap'), 0);
            } finally {
                await reader('restoreListener');
            }
            await waitFor('listener ready while initial read is held', () => reader('initialReaderReconnected'), true);
            await reader('releaseInitialRead');
            await client.waitForFunction(() => boundarySub.ready());
            await checkAfterMarker(group);
        } finally {
            await reader('releaseInitialRead');
            await stop();
            await waitFor('startup channel released', counts, [0, 0]);
        }
    }

    const status = () => reader('reloadStatus');
    async function reconnect(holdResubscribe = false, whileOffline) {
        await reader('disconnectListener');
        try {
            await waitFor('reader disconnected from both channels', counts, [0, 0]);
            if (whileOffline) await whileOffline();
            if (holdResubscribe) await reader('holdResubscribe');
        } finally {
            await reader('restoreListener');
        }
        if (holdResubscribe) {
            await waitFor('second connection before SUBSCRIBE', () => reader('beforeResubscribe'), true);
        } else {
            await waitFor('reader restored on both channels', counts, [1, 1]);
        }
    }

    async function startProbe(id) {
        // One browser clock spans the writer RPC, Redis delivery, DDP and local Minimongo callback.
        const recipients = await client.evaluate(id => {
            const sample = { elapsed: null };
            window.channelSample = sample;
            sample.handle = crossInstance.probes.find(id).observeChanges({
                added() { sample.elapsed = performance.now() - sample.start; },
            });
            sample.start = performance.now();
            return window.peer.callAsync('writeAndNotify', 'probe', id, 'insert');
        }, id);
        assert.equal(recipients, 1, 'probe channel recipients');
    }
    async function finishProbe() {
        await client.waitForFunction(() => channelSample.elapsed !== null);
        return client.evaluate(() => {
            channelSample.handle.stop();
            const ms = channelSample.elapsed;
            delete window.channelSample;
            return ms;
        });
    }
    async function measureOtherChannel(seed) {
        const samples = [];
        for (let i = 0; i < 3; i++) {
            await startProbe('baseline-' + i);
            const baselineMs = await finishProbe();
            await reader('holdNextReload');
            try {
                await reconnect();
                await waitFor('slow primary snapshot', async () => (await status()).snapshot, [seed]);
                const id = 'during-reload-' + i;
                await startProbe(id);
                await waitFor('independent notification reached reader',
                    async () => (await status()).received.includes('probe/' + id), true);
                // Inject a known delay for measurement. Delivery and recovery have separate acknowledgements.
                await delay(250);
                const beforeRelease = await client.evaluate(() => channelSample.elapsed !== null);
                await reader('releaseReload');
                const heldMs = await finishProbe();
                await waitFor('one completed recovery', async () => (await status()).completed, 1);
                samples.push({ baselineMs, heldMs, beforeRelease });
            } finally {
                await reader('clearReloadGate');
            }
        }
        console.log('Independent channel latency (ms), 250 ms injected hold', JSON.stringify(samples));
    }

    async function repeatedDisconnect(afterRead) {
        const group = afterRead ? 'repeat' : 'repeat-control';
        assert.equal(await publish(group, group + '-seed'), 0);
        const subscribeProbe = () => client.evaluate(() => { window.probeSub = crossInstance.subscribe('channelProbes'); });
        if (afterRead) {
            await subscribe(group);
            await waitFor('primary channel attached first', counts, [1, 0]);
            await subscribeProbe();
        } else {
            // Recover the probe cursor before holding the primary cursor for the latency samples.
            await subscribeProbe();
            await waitFor('probe channel attached first', counts, [0, 1]);
            await subscribe(group);
        }
        await client.waitForFunction(() => boundarySub.ready() && probeSub.ready());
        await waitFor('separate Redis channels active', counts, [1, 1]);
        await waitFor('initial repeat view', rows, expected([group + '-seed']));
        const connection = await client.evaluate(() => crossInstance.callReader('instance'));
        try {
            if (!afterRead) await measureOtherChannel(group + '-seed');
            await reader('holdNextReload');
            try {
                await reconnect();
                await waitFor('first reload outstanding', async () => (await status()).snapshot, [group + '-seed']);
                assert.equal((await status()).completed, 0, 'configured connect hook waits for recovery');
                async function finishFirstRecovery() {
                    await reader('releaseReload');
                    await waitFor('first recovery finished while offline', async () => (await status()).completed, 1);
                }
                await reconnect(true, afterRead ? finishFirstRecovery : undefined);
                assert.deepEqual(await counts(), [0, 0]);
                if (afterRead && (await status()).started >= 2) {
                    // A recovery that starts on Redis ready can safely take its second snapshot later.
                    await waitFor('in-flight second Mongo read returned', async () => (await status()).reads.length >= 2, true);
                }
                const beforePublish = await status();
                const recipients = await publish(group, group + '-gap');
                assert.equal(recipients, 0, 'pre-resubscription recipients');
                assert.deepEqual(await writer('boundaryRows', group), expected([group + '-seed', group + '-gap']));
                console.log('Pre-resubscription PUBLISH recipients', recipients, 'snapshots before write', JSON.stringify(beforePublish.reads));
                await reader('releaseResubscribe');
                if (!afterRead) await reader('releaseReload');
                await waitFor('both recoveries completed', async () => (await status()).completed, 2);
                await waitFor('both channels acknowledged again', counts, [1, 1]);
                assert.deepEqual(await client.evaluate(() => crossInstance.callReader('instance')), connection);
                await startProbe(group + '-live');
                await finishProbe();
                await checkAfterMarker(group);
            } finally {
                await reader('releaseResubscribe');
                await reader('clearReloadGate');
            }
        } finally {
            await stop();
            await client.evaluate(() => probeSub.stop());
            await waitFor('both channel subscriptions released', counts, [0, 0]);
        }
    }

    try {
        await Promise.all([admin, client, joining].map(async page => {
            await page.goto(readerUrl);
            await page.waitForFunction(() => window.crossInstance && Meteor.status().connected);
        }));
        await client.evaluate(url => { window.peer = crossInstance.connectWriter(url); }, writerUrl);
        await client.waitForFunction(() => window.peer.status().connected);
        const [a, b] = await Promise.all([reader('instance'), writer('instance')]);
        assert.equal(a.role, 'reader');
        assert.equal(b.role, 'writer');
        assert.notEqual(a.pid, b.pid);
        if (!mode) await initialHandoff(false);
        if (!mode || mode === '--initial-handoff') {
            await initialHandoff(true);
            await failedInitialRead();
            await reconnectDuringInitialRead();
        }
        if (!mode) await repeatedDisconnect(false);
        if (!mode || mode === '--pre-resubscribe') await repeatedDisconnect(true);
    } finally {
        await Promise.all([admin.close(), client.close(), joining.close()]);
    }
};
