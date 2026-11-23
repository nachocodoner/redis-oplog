import { assert } from 'chai';
import { Mongo } from 'meteor/mongo';

describe('Collection hooks and Redis observers', function () {
    for (const [route, options] of [
        ['collection', {}],
        ['channel', { channel: 'hooks-channel' }],
        ['namespace', { namespace: 'hooks-namespace' }],
    ]) {
        const collection = new Mongo.Collection('test_redis_collection_hooks_' + route);

        it('runs each CRUD hook once and delivers changes on the ' + route + ' route', async function () {
            await collection.removeAsync({}, options);
            const calls = [];
            const hooks = [];
            const events = [];
            const handle = await collection.find({}, options).observeChanges({
                added(id, fields) { events.push(['added', id, fields]); },
                changed(id, fields) { events.push(['changed', id, fields]); },
                removed(id) { events.push(['removed', id]); },
            });
            try {
                for (const operation of ['insert', 'update', 'remove']) {
                    for (const timing of ['before', 'after']) {
                        hooks.push(collection[timing][operation](() => { calls.push(timing + '.' + operation); }));
                    }
                }
                const writeOptions = { ...options, optimistic: true };
                const id = await collection.insertAsync({ someData: true }, writeOptions);
                await collection.updateAsync(id, { $set: { someData: false } }, writeOptions);
                await collection.removeAsync(id, writeOptions);
                assert.deepEqual(calls, [
                    'before.insert', 'after.insert', 'before.update', 'after.update', 'before.remove', 'after.remove',
                ]);
                assert.deepEqual(events, [
                    ['added', id, { someData: true }], ['changed', id, { someData: false }], ['removed', id],
                ]);
            } finally {
                await handle.stop();
                for (const hook of hooks) hook.remove();
                await collection.removeAsync({}, options);
            }
        });
    }
});
