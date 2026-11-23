import { Meteor } from 'meteor/meteor';
import { Mongo } from 'meteor/mongo';
import { Random } from 'meteor/random';
import { Tinytest } from 'meteor/tinytest';

Tinytest.addAsync('observeChanges - comparison driver is active', async function (test) {
    const collection = new Mongo.Collection(Random.id());
    const handle = await collection.find({ active: true }).observeChanges({ added() {} });
    try {
        test.equal(Meteor.release, 'METEOR@3.6.1');
        const driver = handle._multiplexer._observeDriver;
        const redis = Package['cultofcoders:redis-oplog'];
        if (process.env.OBSERVER_COMPARISON === 'redis') {
            test.isTrue(redis?.Config.isInitialized === true);
            test.equal(driver.constructor.name, 'RedisOplogObserveDriver');
        } else {
            test.isTrue(redis === undefined);
            test.isTrue(driver._usesChangeStreams === true);
        }
    } finally {
        await handle.stop();
    }
});
