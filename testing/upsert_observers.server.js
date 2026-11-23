import { assert } from 'chai';
import { Mongo } from 'meteor/mongo';
import { Config } from 'meteor/cultofcoders:redis-oplog';

describe('Matched upsert observers', function () {
    const multiUpsertRows = new Mongo.Collection('upsert_observers_multi');

    it('notifies every matched ID when a multi-upsert moves rows out of its selector', async function () {
        const seed = { pushToRedis: false };
        await multiUpsertRows.removeAsync({}, seed);
        for (const _id of ['a', 'b']) await multiUpsertRows.insertAsync({ _id, active: true }, seed);
        const removed = [];
        const handle = await multiUpsertRows.find({ active: true }).observeChanges({ removed(id) { removed.push(id); } });
        try {
            const count = await multiUpsertRows.updateAsync({ active: true }, { $set: { active: false } },
                { upsert: true, multi: true, optimistic: true });
            assert.strictEqual(count, 2);
            assert.deepEqual(removed.sort(), ['a', 'b']);
            assert.deepEqual(await multiUpsertRows.find({ active: true }).fetchAsync(), []);
        } finally {
            await handle.stop();
            await multiUpsertRows.removeAsync({}, seed);
        }
    });

    for (const [mode, settings, expectedDoc] of [
        ['id', {}, { _id: 'moving' }],
        ['previous', { shouldIncludePrevDocument: true }, { _id: 'moving', stage: 'old' }],
        ['full', { protectAgainstRaceConditions: false }, { _id: 'moving', stage: 'new' }],
    ]) {
        const collection = new Mongo.Collection('upsert_observers_' + mode);
        collection.configureRedisOplog(settings);

        it('moves membership and sends the ' + mode + ' payload when the selector changes', async function () {
            await collection.removeAsync({}, { pushToRedis: false });
            await collection.insertAsync({ _id: 'moving', stage: 'old' }, { pushToRedis: false });
            const views = { old: new Set(), new: new Set() };
            const handles = [];
            let receive;
            const notification = new Promise(resolve => { receive = resolve; });
            const handler = message => { if (message.e === 'u') receive(message); };
            await Config.pubSubManager.subscribe(collection._name, handler);
            try {
                for (const stage of Object.keys(views)) {
                    handles.push(await collection.find({ stage }).observeChanges({
                        added(id) { views[stage].add(id); },
                        removed(id) { views[stage].delete(id); },
                    }));
                }
                assert.deepEqual([...views.old], ['moving']);
                assert.deepEqual([...views.new], []);
                const result = await collection.upsertAsync({ _id: 'moving', stage: 'old' },
                    { $set: { stage: 'new' } }, { optimistic: true });
                assert.deepEqual(result, { numberAffected: 1 });
                assert.equal((await collection.findOneAsync('moving')).stage, 'new');
                assert.deepEqual([...views.old], []);
                assert.deepEqual([...views.new], ['moving']);
                const message = await notification;
                assert.deepEqual(message.d, expectedDoc);
                assert.deepEqual(message.f, ['stage']);
            } finally {
                Config.pubSubManager.unsubscribe(collection._name, handler);
                for (const handle of handles) await handle.stop();
                await collection.removeAsync({}, { pushToRedis: false });
            }
        });
    }
});
