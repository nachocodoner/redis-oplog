import { Meteor } from 'meteor/meteor';
import { Mongo, MongoInternals } from 'meteor/mongo';
import { RedisOplog, SyntheticMutator, getRedisPusher, getRedisListener } from 'meteor/cultofcoders:redis-oplog';
import { writeFileSync, renameSync } from 'node:fs';

const name = 'cross_instance_items';
const probeName = 'cross_instance_probes';
const readerSocket = process.env.TEST_ROLE === 'reader' && process.env.TEST_READER_SOCKET;
let reloadGate, initialGate, resubscribeGate;
RedisOplog.init({
    redis: readerSocket ? { path: readerSocket } : { host: '127.0.0.1', port: Number(process.env.TEST_REDIS_PORT) },
    retryIntervalMs: 100,
    redisExtras: {
        events: {
            connect() {
                // The listener invokes this hook after awaiting the reconnect reload.
                if (reloadGate) reloadGate.completed++;
            },
        },
    },
});
const items = new Mongo.Collection(name);
const probes = new Mongo.Collection(probeName);

Meteor.publish('visibleItems', function () {
    return items.find({ visible: true }, { fields: { value: 1 } });
});
const rank = { value: -1, _id: 1 };
Meteor.publish('topItem', function () {
    return items.find({ visible: true }, { fields: { value: 1 }, sort: rank, limit: 1 });
});
Meteor.publish('boundaryItems', group => {
    if (initialGate?.group === group) initialGate.publications++;
    return items.find({ group }, { fields: { value: 1 } });
});
Meteor.publish('channelProbes', () => probes.find({}, { fields: { value: 1 } }));
Meteor.publish('skippedItems', () => items.find({ group: 'upsert-window' }, {
    sort: { value: 1, _id: 1 }, skip: 1, fields: { value: 1 },
}));
Meteor.publish('itemTitles', () => items.find({ titled: true }, { fields: { title: 1, shared: 1 } }));
Meteor.publish('itemScores', () => items.find({ scored: true }, { fields: { score: 1, shared: 1 } }));

const registration = new Meteor.EnvironmentVariable();
Meteor.publish('observerContext', async function () {
    const cursor = items.find({ contextProbe: true }, { fields: { value: 1 } });
    const handle = await registration.withValue(this.connection.id, () => cursor.observeChanges({
        added: (id, fields) => this.added(name, id, { ...fields, registration: registration.get() }),
        changed: (id, fields) => this.changed(name, id, { ...fields, registration: registration.get() }),
        removed: id => this.removed(name, id),
    }));
    this.onStop(() => handle.stop());
    this.ready();
});

function itemObservers() {
    const { _observeMultiplexers } = MongoInternals.defaultRemoteCollectionDriver().mongo;
    return Object.values(_observeMultiplexers)
        .filter(mux => mux._observeDriver?._cursorDescription?.collectionName === name);
}

Meteor.methods({
    instance() {
        return { role: process.env.TEST_ROLE, pid: process.pid, connection: this.connection.id };
    },
    storedView() {
        return items.rawCollection().find({ visible: true }, { projection: { value: 1 } }).sort({ _id: 1 }).toArray();
    },
    storedTopItem() {
        return items.rawCollection().find({ visible: true }, { projection: { value: 1 } }).sort(rank).limit(1).toArray();
    },
    boundaryRows(group) {
        return items.rawCollection().find({ group }, { projection: { value: 1 } }).sort({ _id: 1 }).toArray();
    },
    upsertWindowItem(id, value, leave = false) {
        if (process.env.TEST_ROLE !== 'writer') throw new Error('Expected the writer');
        return items.upsertAsync({ _id: id, group: 'upsert-window' },
            { $set: { value, group: leave ? 'finished' : 'upsert-window' } });
    },
    async writeAndNotify(group, id, operation) {
        if (process.env.TEST_ROLE !== 'writer') throw new Error('Expected the writer');
        const collection = group === 'probe' ? probes : items;
        if (operation === 'remove') {
            const result = await collection.rawCollection().deleteOne({ _id: id, group });
            if (result.deletedCount !== 1) throw new Error('Expected to remove the seeded document');
        } else {
            await collection.rawCollection().insertOne({ _id: id, group, value: 1, hidden: true });
        }
        const event = operation === 'remove' ? 'r' : 'i';
        return new Promise((resolve, reject) => {
            getRedisPusher().publish(collection._name, JSON.stringify({ e: event, d: { _id: id } }),
                (error, count) => error ? reject(error) : resolve(count));
        });
    },
    boundaryChannels() {
        return new Promise((resolve, reject) => {
            getRedisPusher().pubsub('numsub', name, probeName, (error, reply) => {
                if (error) reject(error);
                else resolve([Number(reply[1]), Number(reply[3])]);
            });
        });
    },
    holdInitialRead(group, fail = false) {
        if (initialGate) throw new Error('Initial read already held');
        const find = items.find;
        let release;
        const held = new Promise(resolve => { release = resolve; });
        const gate = { group, publications: 0, ids: null, messages: [], reconnected: false, release, restore: () => { items.find = find; } };
        gate.onReady = () => { gate.reconnected = true; };
        gate.onMessage = (channel, payload) => {
            if (channel !== name) return;
            const { e, d } = JSON.parse(payload);
            gate.messages.push([e, d._id]);
        };
        getRedisListener().once('ready', gate.onReady);
        getRedisListener().on('message', gate.onMessage);
        initialGate = gate;
        items.find = function (selector, ...args) {
            const cursor = find.call(this, selector, ...args);
            if (selector?.group === group) {
                const fetch = cursor.fetchAsync;
                cursor.fetchAsync = async function (...params) {
                    cursor.fetchAsync = fetch;
                    const rows = await fetch.apply(this, params);
                    if (gate.ids === null) {
                        gate.ids = rows.map(row => row._id).sort();
                        gate.restore();
                        await held;
                        if (fail) throw new Meteor.Error('test-initial-read', 'Injected initial query failure');
                    }
                    return rows;
                };
            }
            return cursor;
        };
    },
    initialReadIds() {
        return initialGate.ids;
    },
    initialPublications() {
        return initialGate.publications;
    },
    initialReaderReconnected() {
        return initialGate.reconnected;
    },
    initialMessages() {
        return initialGate.messages;
    },
    releaseInitialRead() {
        if (initialGate) {
            getRedisListener().removeListener('ready', initialGate.onReady);
            getRedisListener().removeListener('message', initialGate.onMessage);
            initialGate.restore();
            initialGate.release();
            initialGate = null;
        }
    },
    holdResubscribe() {
        if (resubscribeGate) throw new Error('Resubscription already held');
        // node-redis 3 sends its remembered SUBSCRIBE commands from on_ready, after connect.
        const listener = getRedisListener();
        const onReady = listener.on_ready;
        const gate = { entered: false, dispatchTurn: false, resume() {
            listener.on_ready = onReady;
            if (gate.entered) onReady.call(listener);
        } };
        resubscribeGate = gate;
        listener.on_ready = () => {
            gate.entered = true;
            // Let recovery already scheduled by the connect event start before the test samples it.
            setImmediate(() => { gate.dispatchTurn = true; });
        };
    },
    beforeResubscribe() {
        return !!resubscribeGate?.dispatchTurn && getRedisListener().connected;
    },
    releaseResubscribe() {
        if (resubscribeGate) {
            resubscribeGate.resume();
            resubscribeGate = null;
        }
    },
    disconnectListener() {
        if (!readerSocket) throw new Error('Only the reader has a listener socket for this test');
        // Keep the existing command connection alive. Refuse new connections until the test restores the path.
        renameSync(readerSocket, readerSocket + '.offline');
        getRedisListener().stream.destroy();
    },
    restoreListener() {
        if (!readerSocket) throw new Error('Only the reader has a listener socket for this test');
        renameSync(readerSocket + '.offline', readerSocket);
    },
    holdNextReload() {
        const observers = itemObservers();
        if (reloadGate || observers.length !== 1) throw new Error('Expected one active observer');
        const { cursor } = observers[0]._observeDriver.observableCollection;
        const fetch = cursor.fetchAsync;
        let release;
        const held = new Promise(resolve => { release = resolve; });
        const listener = getRedisListener();
        function onMessage(channel, payload) {
            if (channel === probeName) gate.received.add('probe/' + JSON.parse(payload).d._id);
            if (channel !== name) return;
            const event = JSON.parse(payload);
            gate.received.add([event.s ? 'synthetic' : 'stored', event.e, event.d._id].join('/'));
        }
        const gate = {
            snapshot: null, started: 0, reads: [], received: new Set(), completed: 0, release,
            dispose() {
                listener.removeListener('message', onMessage);
                cursor.fetchAsync = fetch;
                release();
            },
        };
        reloadGate = gate;
        listener.on('message', onMessage);
        cursor.fetchAsync = async function (...args) {
            gate.started++;
            const documents = await fetch.apply(this, args);
            const ids = documents.map(doc => doc._id).sort();
            gate.reads.push(ids);
            if (gate.snapshot === null) {
                gate.snapshot = ids;
                await held;
            }
            return documents;
        };
    },
    reloadStatus() {
        const { snapshot, started, reads, received, completed } = reloadGate;
        return { snapshot, started, reads, received: [...received], completed, finished: completed > 0 };
    },
    releaseReload() {
        reloadGate.release();
    },
    clearReloadGate() {
        reloadGate.dispose();
        reloadGate = null;
    },
    optimisticReloadWrite() {
        if (!readerSocket) throw new Error('Expected a local write on the observing instance');
        return items.updateAsync('local', { $set: { value: 5 } }, { optimistic: true });
    },
    async writeDuringDisconnect() {
        if (process.env.TEST_ROLE !== 'writer') throw new Error('Writes must go through the writer');
        const collection = items.rawCollection();
        const insert = await collection.insertOne({ _id: 'added', visible: true, value: 3, privateField: 'hidden' });
        const update = await collection.updateOne({ _id: 'row' }, { $set: { value: 4 } });
        const remove = await collection.deleteOne({ _id: 'removed' });
        // External writes use the documented Redis envelopes. Await the broker's recipient count for each.
        const recipients = [];
        for (const message of [
            { e: 'i', d: { _id: 'added' } }, { e: 'u', d: { _id: 'row' }, f: ['value'] },
            { e: 'r', d: { _id: 'removed' } },
        ]) {
            recipients.push(await new Promise((resolve, reject) => {
                getRedisPusher().publish(name, JSON.stringify(message), (error, count) => {
                    if (error) reject(error);
                    else resolve(count);
                });
            }));
        }
        return { inserted: insert.insertedId, updated: update.modifiedCount, removed: remove.deletedCount, recipients };
    },
    async observerState() {
        const queries = itemObservers();
        const subscriptions = await new Promise((resolve, reject) => {
            getRedisPusher().pubsub('numsub', name, (error, reply) => {
                if (error) reject(error);
                else resolve(Number(reply[1]));
            });
        });
        return {
            drivers: queries.map(mux => mux._observeDriver.constructor.name),
            handles: queries.map(mux => Object.keys(mux._handles).length),
            subscriptions,
        };
    },
    async write(operation, id = 'row') {
        if (process.env.TEST_ROLE !== 'writer') throw new Error('Writes must go through the writer');
        switch (operation) {
            case 'insert':
                return items.insertAsync({ _id: id, visible: true, value: 1, privateField: 'hidden' });
            case 'insertContext':
                return items.insertAsync({ _id: id, contextProbe: true, value: 1 });
            case 'update':
                return items.updateAsync(id, { $set: { value: 2 } });
            case 'clear':
                return items.updateAsync(id, { $unset: { value: 1 } });
            case 'leave':
                return items.updateAsync(id, { $set: { visible: false } });
            case 'enter':
                return items.updateAsync(id, { $set: { visible: true, value: 3 } });
            case 'remove':
                return items.removeAsync(id);
            case 'incrementSynthetic':
                return SyntheticMutator.update(name, id, { $inc: { value: 10 } });
            case 'insertForBoth':
                return items.insertAsync({ _id: id, titled: true, scored: true, title: 'first', score: 1, shared: 'both', hidden: true });
            case 'updateBoth':
                return items.updateAsync(id, { $set: { title: 'second', score: 2 }, $unset: { shared: 1 } });
            case 'leaveTitles':
                return items.updateAsync(id, { $set: { titled: false, shared: 'scores' } });
            case 'enterTitles':
                return items.updateAsync(id, { $set: { titled: true } });
            default:
                throw new Error('Unknown write ' + operation);
        }
    },
});

// The writer reuses the reader's disposable Meteor MongoDB and built application.
Meteor.startup(() => {
    const file = process.env.TEST_READY_FILE;
    writeFileSync(file + '.tmp', process.env.MONGO_URL, { mode: 0o600 });
    renameSync(file + '.tmp', file);
});
