import { assert } from 'chai';
import { Meteor } from 'meteor/meteor';
import { Mongo } from 'meteor/mongo';

const Collection = new Mongo.Collection('test_observe_callbacks');

describe('Shared observeChanges callbacks', function () {
    const options = { optimistic: true };
    const rows = new Mongo.Collection('test_shared_observe_callbacks');
    let handles;

    beforeEach(async function () {
        handles = [];
        await rows.removeAsync({}, options);
    });

    afterEach(async function () {
        for (const handle of handles) await handle.stop();
        await rows.removeAsync({}, options);
    });

    it('isolates nested initial, cached and live fields and uses Meteor callback signatures', async function () {
        const cursor = rows.find({}, { fields: { nested: 1 } });
        await rows.insertAsync({ _id: 'initial', nested: { value: 1 }, hidden: true }, options);
        const mutate = (id, fields) => { fields.nested.value = -1; };
        handles.push(await cursor.observeChanges({ added: mutate, changed: mutate }));
        const events = [];
        handles.push(await cursor.observeChanges({
            added(...args) { events.push(['added', ...args]); },
            changed(...args) { events.push(['changed', ...args]); },
            removed(...args) { events.push(['removed', ...args]); },
        }));
        await rows.insertAsync({ _id: 'live', nested: { value: 2 }, hidden: true }, options);
        await rows.updateAsync('live', { $set: { 'nested.value': 3 } }, options);
        await rows.removeAsync('live', options);
        assert.strictEqual(handles[0]._multiplexer, handles[1]._multiplexer);
        assert.deepEqual(events, [
            ['added', 'initial', { nested: { value: 1 } }],
            ['added', 'live', { nested: { value: 2 } }],
            ['changed', 'live', { nested: { value: 3 } }],
            ['removed', 'live'],
        ]);
    });

    it('runs each shared callback in the environment where its handle was registered', async function () {
        const environment = new Meteor.EnvironmentVariable();
        const events = { first: [], second: [] };
        const cursor = rows.find();
        for (const label of Object.keys(events)) {
            const record = async () => {
                await Promise.resolve();
                events[label].push(environment.get());
            };
            handles.push(await environment.withValue(label, () => cursor.observeChanges({
                added: record, changed: record, removed: record,
            })));
        }
        await environment.withValue('writer', async () => {
            await rows.insertAsync({ _id: 'local', value: 1 }, options);
            await rows.updateAsync('local', { $set: { value: 2 } }, options);
            await rows.removeAsync('local', options);
        });
        assert.deepEqual(events, { first: Array(3).fill('first'), second: Array(3).fill('second') });
    });
});

describe('Observe callbacks should work', function () {
    it('Should work', async function (done) {
        await Collection.removeAsync({});

        const context = 'observe-callbacks';

        let _id;
        let inAdded = false;
        const handler = await Collection.find({context}).observe({
            async added(newDoc) {
                assert.isObject(newDoc);
                assert.equal(newDoc.number, 10);
                await Collection.updateAsync(newDoc._id, {
                    $set: {number: 20}
                });
            },
            async changed(newDoc, oldDoc) {
                if (oldDoc.number === 10) {
                    assert.isObject(newDoc);
                    assert.isObject(oldDoc);
                    assert.equal(newDoc.number, 20);
                    assert.equal(oldDoc.number, 10);

                    await Collection.removeAsync(newDoc._id);
                }
            },
            async removed(oldDoc) {
                assert.isObject(oldDoc);
                assert.equal(oldDoc.number, 20);
                await handler.stop();
                done();
            }
        });

        assert.isFunction(handler.stop);
        await Collection.insertAsync({context, number: 10});
    });

    it ('Should not be triggered if no changes are detected', async function (done) {
        await Collection.removeAsync({});
        const _id = await Collection.insertAsync({number: 10});

        let inChanged = false;
        const handler = await Collection.find().observe({
            changed(newDoc, oldDoc) {
                inChanged = true;
            }
        });

        await Collection.updateAsync(_id, {
            $set: {number: 10}
        });

        setTimeout(() => {
            assert.isFalse(inChanged);
            done();
        }, 100)
    })
});
