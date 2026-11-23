const assert = require('node:assert/strict');
const waitFor = require('./wait-for.cjs');

module.exports = async function exercise(browser, readerUrl, writerUrl, overlap) {
    const [first, second] = await Promise.all([browser.newPage(), browser.newPage()]);
    for (const page of [first, second]) {
        await page.goto(readerUrl);
        await page.waitForFunction(() => window.crossInstance && Meteor.status().connected);
    }
    const observerState = () => first.evaluate(() => crossInstance.callReader('observerState'));
    const state = (handles, subscriptions) => ({
        drivers: handles.map(() => 'RedisOplogObserveDriver'), handles, subscriptions,
    });
    const view = (page, rows, label = 'subscriber documents') => waitFor(label,
        () => page.evaluate(() => crossInstance.items.find({}, { sort: { _id: 1 } }).fetch()), rows);
    const row = value => [{ _id: 'row', value }];
    async function subscribe(page, name = 'visibleItems') {
        await page.evaluate(name => { window.subscription = crossInstance.subscribe(name); }, name);
        await page.waitForFunction(() => subscription.ready());
    }

    await first.evaluate(url => { window.writer = crossInstance.connectWriter(url); }, writerUrl);
    await first.waitForFunction(() => writer.status().connected);
    const [reader, writer] = await first.evaluate(() => Promise.all([
        crossInstance.callReader('instance'), window.writer.callAsync('instance'),
    ]));
    assert.equal(reader.role, 'reader');
    assert.equal(writer.role, 'writer');
    assert.notEqual(reader.pid, writer.pid);
    const writeOnPeer = (operation, id) => first.evaluate(
        (action, key) => window.writer.callAsync('write', action, key), operation, id);
    const storedView = () => first.evaluate(() => window.writer.callAsync('storedView'));
    async function write(operation, expectedResult, expectedRows, id = 'row') {
        assert.equal(await writeOnPeer(operation, id), expectedResult, operation);
        assert.deepEqual(await storedView(), expectedRows, 'MongoDB after ' + operation);
    }

    await waitFor('before observing', observerState, state([], 0));
    await subscribe(first);
    await waitFor('first Redis subscription', observerState, state([1], 1));
    await view(first, []);
    await write('insert', 'row', row(1));
    await view(first, row(1));

    await subscribe(second);
    await waitFor('shared observer', observerState, state([2], 1));
    await view(second, row(1));
    await write('update', 1, row(2));
    await Promise.all([view(first, row(2)), view(second, row(2))]);
    await first.evaluate(() => subscription.stop());
    await waitFor('remaining observer', observerState, state([1], 1));
    await view(first, []);

    for (const [operation, rows] of [
        ['clear', [{ _id: 'row' }]], ['leave', []], ['enter', row(3)], ['remove', []],
    ]) {
        await write(operation, 1, rows);
        await view(second, rows);
        await view(first, []);
    }

    const beforeGap = [{ _id: 'removed', value: 1 }, { _id: 'row', value: 1 }];
    const afterGap = [{ _id: 'added', value: 3 }, { _id: 'row', value: 4 }];
    const subscriber = await second.evaluate(() => crossInstance.callReader('instance'));
    await write('insert', 'row', row(1));
    await write('insert', 'removed', beforeGap, 'removed');
    await view(second, beforeGap);
    await first.evaluate(() => crossInstance.callReader('disconnectListener'));
    try {
        await waitFor('listener offline with one active handle', observerState, state([1], 0));
        const writes = await first.evaluate(() => window.writer.callAsync('writeDuringDisconnect'));
        assert.deepEqual(writes, { inserted: 'added', updated: 1, removed: 1, recipients: [0, 0, 0] });
        assert.deepEqual(await first.evaluate(() => window.writer.callAsync('storedView')), afterGap);
        await view(second, beforeGap, 'subscriber view during the Redis outage');
    } finally {
        await first.evaluate(() => crossInstance.callReader('restoreListener'));
    }
    await waitFor('reader resubscribed', observerState, state([1], 1));
    await view(second, afterGap, 'subscriber view after Redis reconnect');
    assert.deepEqual(await second.evaluate(() => crossInstance.callReader('instance')), subscriber);
    await view(first, []);

    const live = [{ _id: 'added', value: 3 }, { _id: 'row', value: 2 }];
    await write('update', 1, live);
    await view(second, live);
    await view(first, []);

    // The control and overlap schedules differ only in when the snapshot is released.
    const callReader = method => first.evaluate(name => crossInstance.callReader(name), method);
    const reloadState = async field => (await callReader('reloadStatus'))[field];
    async function reconnectWithSnapshot(ids) {
        await callReader('disconnectListener');
        try {
            await waitFor('listener disconnected before gated reload', observerState, state([1], 0));
        } finally {
            await callReader('restoreListener');
        }
        await waitFor('snapshot fetched from MongoDB', () => reloadState('snapshot'), ids);
        await waitFor('reader subscribed during gated reload', observerState, state([1], 1));
    }
    async function finishReload() {
        await callReader('releaseReload');
        await waitFor('reconnect reload completed', () => reloadState('finished'), true);
    }
    const seed = [live[0], { _id: 'local', value: 1 }, live[1]];
    await write('insert', 'local', seed, 'local');
    await view(second, seed);
    const persisted = [{ _id: 'added', value: 2 }, { _id: 'local', value: 5 }, { _id: 'new', value: 1 }];
    async function recoveryWrites() {
        assert.equal(await writeOnPeer('insert', 'new'), 'new');
        assert.equal(await writeOnPeer('update', 'added'), 1);
        assert.equal(await writeOnPeer('remove', 'row'), 1);
        await writeOnPeer('incrementSynthetic', 'added');
        const expected = ['stored/i/new', 'stored/u/added', 'stored/r/row', 'synthetic/u/added'];
        await waitFor('reader received the four notifications', async () => {
            const received = await reloadState('received');
            return expected.every(event => received.includes(event));
        }, true);
        // Its Mongo write can commit while the optimistic observer callback is still waiting.
        await second.evaluate(() => {
            window.localWrite = crossInstance.callReader('optimisticReloadWrite')
                .then(count => ({ count }), error => ({ error: error.message }));
        });
        await waitFor('all persistent writes committed', storedView, persisted);
    }
    await callReader('holdNextReload');
    try {
        await second.evaluate(() => {
            const events = [];
            const observer = crossInstance.items.find('row').observeChanges({
                added() { events.push('added'); },
                removed() { events.push('removed'); },
            });
            window.finishReloadTrace = () => {
                observer.stop();
                delete window.finishReloadTrace;
                return events;
            };
        });
        await reconnectWithSnapshot(['added', 'local', 'row']);
        if (overlap) await recoveryWrites();
        await finishReload();
        if (!overlap) await recoveryWrites();
        assert.deepEqual(await second.evaluate(() => window.localWrite), { count: 1 });

        // A change to a different ID can cross the same channel without repairing a stale target row.
        const settled = [persisted[0], persisted[1], { _id: 'marker', value: 1 }, persisted[2]];
        await write('insert', 'marker', settled, 'marker');
        await second.waitForFunction(() => !!crossInstance.items.findOne('marker'));
        assert.deepEqual(await second.evaluate(() => crossInstance.callReader('instance')), subscriber);
        await view(first, []);
        await view(second, [{ _id: 'added', value: 12 }, ...settled.slice(1)],
            'subscriber after reload and independent live marker');
    } finally {
        try {
            const trace = await second.evaluate(() => window.finishReloadTrace && window.finishReloadTrace());
            console.log('Browser membership events for row:', trace);
        } finally {
            await callReader('clearReloadGate');
        }
    }
    await second.evaluate(() => subscription.stop());
    await waitFor('last observer released', observerState, state([], 0));
    await view(second, []);

    await subscribe(second, 'topItem');
    await waitFor('sorted window subscribed', observerState, state([1], 1));
    await view(second, [{ _id: 'local', value: 5 }]);
    await callReader('holdNextReload');
    try {
        await reconnectWithSnapshot(['local']);
        if (!overlap) await finishReload();
        assert.equal(await writeOnPeer('leave', 'local'), 1);
        await waitFor('reader received the window change',
            async () => (await reloadState('received')).includes('stored/u/local'), true);
        const winner = await first.evaluate(() => window.writer.callAsync('storedTopItem'));
        assert.deepEqual(winner, [{ _id: 'added', value: 2 }]);
        if (overlap) await finishReload();
        await view(second, winner, 'sorted window after recovery');
    } finally {
        await callReader('clearReloadGate');
        await second.evaluate(() => subscription.stop());
    }
    await waitFor('window observer released', observerState, state([], 0));

    // Two publications contribute different fields of the same document to one DDP connection.
    const id = 'overlapping-projections';
    assert.equal(await writeOnPeer('insertForBoth', id), id);
    await subscribe(second, 'itemTitles');
    try {
        await view(second, [{ _id: id, title: 'first', shared: 'both' }]);
        await second.evaluate(() => { window.scores = crossInstance.subscribe('itemScores'); });
        await second.waitForFunction(() => scores.ready());
        await waitFor('distinct projections share one Redis channel', observerState, state([1, 1], 1));
        await view(second, [{ _id: id, title: 'first', score: 1, shared: 'both' }]);
        assert.equal(await writeOnPeer('updateBoth', id), 1);
        await view(second, [{ _id: id, title: 'second', score: 2 }]);
        assert.equal(await writeOnPeer('leaveTitles', id), 1);
        await view(second, [{ _id: id, score: 2, shared: 'scores' }]);
        assert.equal(await writeOnPeer('enterTitles', id), 1);
        await view(second, [{ _id: id, title: 'second', score: 2, shared: 'scores' }]);
        await second.evaluate(() => scores.stop());
        await waitFor('titles still subscribed', observerState, state([1], 1));
        await view(second, [{ _id: id, title: 'second', shared: 'scores' }]);
    } finally {
        await second.evaluate(() => { if (window.scores) scores.stop(); subscription.stop(); });
    }
    await waitFor('both projections released', observerState, state([], 0));
    await view(second, []);

    await subscribe(first, 'observerContext');
    await subscribe(second, 'observerContext');
    try {
        const pages = [first, second];
        const connections = await Promise.all(pages.map(page =>
            page.evaluate(async () => (await crossInstance.callReader('instance')).connection)));
        assert.notEqual(connections[0], connections[1]);
        await waitFor('context observers share one driver', observerState, state([2], 1));
        const id = 'context-row';
        for (const [action, value] of [['insertContext', 1], ['update', 2]]) {
            assert.equal(await writeOnPeer(action, id), action === 'insertContext' ? id : 1);
            await Promise.all(pages.map((page, index) => view(page,
                [{ _id: id, value, registration: connections[index] }], 'registration context after peer ' + action)));
        }
        assert.equal(await writeOnPeer('remove', id), 1);
        await Promise.all(pages.map(page => view(page, [])));
    } finally {
        await Promise.all([first, second].map(page => page.evaluate(() => subscription.stop())));
    }
    await waitFor('context observers released', observerState, state([], 0));

    const upsert = (id, value, leave = false) => first.evaluate((id, value, leave) =>
        window.writer.callAsync('upsertWindowItem', id, value, leave), id, value, leave);
    const a = { _id: 'window-a', value: 2 };
    const b = { _id: 'window-b', value: 3 };
    assert.equal((await upsert(a._id, a.value)).insertedId, a._id);
    assert.equal((await upsert(b._id, b.value)).insertedId, b._id);
    await subscribe(second, 'skippedItems');
    try {
        await waitFor('skip-only Redis observer', observerState, state([1], 1));
        await view(second, [b]);
        assert.equal((await upsert('window-first', 1)).insertedId, 'window-first');
        await view(second, [a, b], 'peer insert before the skip boundary');
        assert.deepEqual(await upsert('window-first', 1, true), { numberAffected: 1 });
        await view(second, [b], 'matched peer upsert moving the skipped document out');
    } finally {
        await second.evaluate(() => subscription.stop());
    }
    await waitFor('skip-only observer released', observerState, state([], 0));
};
