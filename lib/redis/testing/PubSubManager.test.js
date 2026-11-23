import { assert } from 'chai';
import { Meteor } from 'meteor/meteor';
import PubSubManager from '../PubSubManager';

describe('Redis channel attachment', function () {
    it('installs the first handler before acknowledgement and shares one acknowledged subscription', async function () {
        let acknowledge, sent, commands = 0;
        const requested = new Promise(resolve => { sent = resolve; });
        const pubsub = Object.create(PubSubManager.prototype);
        pubsub.queue = new Meteor._AsynchronousQueue();
        pubsub.channelHandlers = {};
        pubsub.listener = {
            subscribe(channel, callback) {
                assert.equal(channel, 'items');
                commands++;
                acknowledge = callback;
                sent();
                return true; // The v3 client reports a socket write here.
            },
        };
        const first = () => {};
        const second = () => {};
        let attached = false;
        const both = Promise.all([pubsub.subscribe('items', first), pubsub.subscribe('items', second)])
            .then(() => { attached = true; });
        await requested;
        await new Promise(resolve => setImmediate(resolve));
        assert.isFalse(attached);
        assert.deepEqual(pubsub.channelHandlers.items, [first]);
        acknowledge(null, 'items');
        await both;
        assert.equal(commands, 1);
        assert.deepEqual(pubsub.channelHandlers.items, [first, second]);
    });
});
