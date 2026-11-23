import { assert } from 'chai';
import { MongoIDMap } from '../../cache/mongoIdMap';
import { RedisSubscriptionManager } from '../RedisSubscriptionManager';

function recoveryFixture() {
    const manager = new RedisSubscriptionManager();
    manager.init();

    let entered, release, fail;
    const reading = new Promise(resolve => { entered = resolve; });
    const snapshot = new Promise((resolve, reject) => { release = resolve; fail = reject; });
    const store = new MongoIDMap();
    const row = { _id: 'row', value: 1 };
    store.set(row._id, row);
    let reads = 0;
    const subscriber = {
        channels: ['items', 'alias'],
        observableCollection: {
            store,
            cursor: { fetchAsync() { reads++; entered(); return snapshot; } },
            add(doc) { store.set(doc._id, doc); },
            change(doc) { store.set(doc._id, doc); },
        },
        process(event, doc) {
            assert.equal(event, 'r');
            store.remove(doc._id);
        },
    };
    for (const channel of subscriber.channels) manager.store[channel] = [subscriber];
    return { manager, store, reading, release: () => release([row]), fail, reads: () => reads };
}

const removal = { e: 'r', d: { _id: 'row' } };

describe('Redis observer startup', function () {
    it('buffers only the starting observer and delivers overlapping synthetic and optimistic events in order', async function () {
        const manager = new RedisSubscriptionManager();
        manager.init();
        function observer(events) {
            return {
                channels: ['items'],
                observableCollection: { collection: {} },
                processSynthetic(event, doc, modifier) { events.push(modifier.$inc.value); },
                process(event) { events.push(event); },
            };
        }
        const existingEvents = [], startingEvents = [];
        const starting = observer(startingEvents);
        manager.starting.set(starting, []);
        manager.store.items = [observer(existingEvents), starting];
        const synthetic = { e: 'u', s: true, d: { _id: 'row' }, m: { $inc: { value: 2 } } };
        await manager.process('items', synthetic, true);
        await manager.process('items', removal);
        assert.deepEqual(existingEvents, [2, 'r']);
        assert.deepEqual(startingEvents, []);
        const handoff = manager.finishInitialization(starting);
        const later = manager.queue.runTask(() => manager.process('items', synthetic, true));
        await Promise.all([handoff, later]);
        assert.deepEqual(startingEvents, [2, 'r', 2]);
        assert.deepEqual(existingEvents, startingEvents);
    });
});

describe('Redis listener recovery', function () {
    it('applies the snapshot once for a multi-channel observer before its queued removal', async function () {
        const fixture = recoveryFixture();
        const { manager } = fixture;
        const recovery = manager.reloadSubscribers();
        await fixture.reading;
        const delivery = manager.queue.runTask(() => manager.process('alias', removal, true));
        // Give the ordered queue a turn while Mongo still holds the old snapshot.
        await new Promise(resolve => setImmediate(resolve));
        fixture.release();
        await Promise.all([recovery, delivery]);
        assert.equal(fixture.reads(), 1);
        assert.isFalse(fixture.store.has('row'));
    });

    it('releases a local delivery if its overlapping recovery read fails', async function () {
        const fixture = recoveryFixture();
        const { manager } = fixture;
        const failure = new Error('failed recovery read');
        const recovery = manager.reloadSubscribers().catch(error => error);
        await fixture.reading;
        const delivery = manager.process('alias', removal);
        try {
            assert.isTrue(fixture.store.has('row'), 'optimistic delivery must wait for the snapshot');
        } finally {
            fixture.fail(failure);
        }
        assert.strictEqual(await recovery, failure);
        await delivery;
        assert.isFalse(fixture.store.has('row'));
    });

    it('skips an observer detached before its queued reload starts', async function () {
        const fixture = recoveryFixture();
        const { manager } = fixture;
        manager.queue.queueTask(() => { manager.store = {}; });
        fixture.release();
        await manager.reloadSubscribers();
        assert.equal(fixture.reads(), 0);
    });
});
