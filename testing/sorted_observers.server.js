import { assert } from 'chai';
import { Mongo } from 'meteor/mongo';
import { LocalCollection } from 'meteor/minimongo';

const rows = new Mongo.Collection('sorted_observer_contract');

describe('Sorted observers', function () {
    beforeEach(() => rows.removeAsync({}));
    afterEach(() => rows.removeAsync({}));

    it('keeps a skip-only window equal to Mongo after writes on both sides of its boundary', async function () {
        const write = { optimistic: true };
        const cursor = rows.find({ active: true }, { skip: 1, sort: { rank: 1, _id: 1 }, projection: { label: 1 } });
        const visible = new Map();
        const handle = await cursor.observeChanges({
            added(id, fields) { visible.set(id, fields.label); },
            changed(id, fields) { visible.set(id, fields.label); },
            removed(id) { visible.delete(id); },
        });
        async function checkWindow() {
            const expected = (await cursor.fetchAsync()).map(doc => [doc._id, doc.label]).sort();
            assert.deepEqual([...visible].sort(), expected);
        }
        try {
            for (const [id, rank] of [['a', 1], ['b', 2], ['c', 3], ['first', 0]]) {
                await rows.insertAsync({ _id: id, active: true, label: id, rank }, write);
                await checkWindow();
            }
            await rows.updateAsync('first', { $set: { active: false } }, write);
            await checkWindow();
            await rows.updateAsync('first', { $set: { active: true } }, write);
            await checkWindow();
            await rows.removeAsync('first', write);
            await checkWindow();
            await rows.updateAsync('c', { $set: { rank: 0 } }, write);
            await checkWindow();
            await rows.updateAsync('a', { $set: { label: 'updated' } }, write);
            await checkWindow();
        } finally {
            await handle.stop();
        }
    });

    it('keeps dedicated ID routing for an unsorted skip-only window', async function () {
        const ids = ['one', 'two'];
        const cursor = rows.find({ _id: { $in: ids } }, { skip: 1, limit: 0 });
        const visible = new Set();
        const handle = await cursor.observeChanges({
            added(id) { visible.add(id); },
            removed(id) { visible.delete(id); },
        });
        try {
            // These writes reach this observer only through the per-ID channels.
            const options = { channel: 'another-channel', optimistic: true };
            for (const id of ids) {
                await rows.insertAsync({ _id: id }, options);
                assert.deepEqual([...visible], (await cursor.fetchAsync()).map(doc => doc._id));
            }
            assert.equal(visible.size, 1);
            await rows.removeAsync(ids.find(id => !visible.has(id)), options);
            assert.equal(visible.size, 0);
        } finally {
            await handle.stop();
        }
    });

    it('uses Redis for an unordered limit and tracks a change to an unprojected sort key', async function () {
        await rows.insertAsync({ _id: 'first', rank: 1, label: 'a' });
        await rows.insertAsync({ _id: 'second', rank: 2, label: 'b' });
        const cursor = rows.find({}, { sort: { rank: 1, _id: 1 }, limit: 1, fields: { label: 1 } });
        const visible = new Set();
        const additions = [];
        const handle = await cursor.observeChanges({
            added(id, fields) { visible.add(id); additions.push([id, fields]); },
            removed(id) { visible.delete(id); },
        });
        try {
            assert.equal(handle._multiplexer._observeDriver.constructor.name, 'RedisOplogObserveDriver');
            assert.deepEqual([...visible], ['first']);
            await rows.updateAsync('second', { $set: { rank: 0 } }, { optimistic: true });
            assert.deepEqual([...visible], ['second']);
            assert.deepEqual(await cursor.fetchAsync(), [{ _id: 'second', label: 'b' }]);
            await rows.removeAsync('second', { optimistic: true });
            assert.deepEqual([...visible], ['first']);
            assert.deepEqual(additions, [
                ['first', { label: 'a' }],
                ['second', { label: 'b' }],
                ['first', { label: 'a' }],
            ]);
        } finally {
            await handle.stop();
        }
    });

    if (LocalCollection._createCollator) {
        it('delegates a sorted locale-aware selector and returns the Mongo membership', async function () {
            await rows.insertAsync({ _id: 'one', label: 'Apple' });
            await rows.insertAsync({ _id: 'two', label: 'apple' });
            const cursor = rows.find({ label: 'apple' }, { sort: { label: 1 }, collation: { locale: 'en', strength: 2 } });
            const ids = [];
            const handle = await cursor.observeChanges({ added(id) { ids.push(id); } });
            try {
                assert.equal(handle._multiplexer._observeDriver.constructor.name, 'PollingObserveDriver');
                assert.deepEqual(ids.sort(), ['one', 'two']);
            } finally {
                await handle.stop();
            }
        });
    }
});
