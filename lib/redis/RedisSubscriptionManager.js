import { Meteor } from "meteor/meteor";
import { Random } from "meteor/random";
import { _ } from "meteor/underscore";
import debug from "../debug";
import { RedisPipe, Events } from "../constants";
import getFieldsOfInterestFromAll from "./lib/getFieldsOfInterestFromAll";
import Config from "../config";
import reload from "../processors/actions/reload";

export class RedisSubscriptionManager {
  init() {
    if (this.isInitialized) {
      return;
    }
    this.uid = Random.id();
    this.queue = new Meteor._AsynchronousQueue();
    this.store = {}; // {channel: [RedisSubscribers]}
    this.channelHandlers = {}; // {channel: {handler, subscribed}}
    this.starting = new Map(); // subscriber -> notifications received during its initial query

    this.isInitialized = true;
  }

  /**
   * Returns all RedisSubscribers regardless of channel
   */
  getAllRedisSubscribers() {
    let redisSubscribers = [];
    for (let channel in this.store) {
      this.store[channel].forEach((_redisSubscriber) =>
        redisSubscribers.push(_redisSubscriber)
      );
    }

    return redisSubscribers;
  }

  async reloadSubscribers() {
    // A single observer may listen on multiple channels. Each snapshot must be
    // applied in the same order as notifications from the Redis listener.
    const initializing = [];
    for (const subscriber of new Set(this.getAllRedisSubscribers())) {
      await this.queue.runTask(async () => {
        const attached = subscriber.channels.some((channel) =>
          this.store[channel]?.includes(subscriber)
        );
        if (!attached) return;
        const pending = this.starting.get(subscriber);
        if (pending) {
          // An observer still waiting for SUBSCRIBE will read Mongo after ready.
          if (subscriber.observableCollection.__isInitialized) {
            pending.push(() => reload(subscriber.observableCollection));
            initializing.push(subscriber.ready);
          }
          return;
        }

        await this.applySnapshotWork(subscriber, () =>
          reload(subscriber.observableCollection)
        );
      });
    }
    await Promise.all(initializing);
  }

  finishInitialization(subscriber) {
    // Take a finite backlog at a position in the live queue. Subsequent Redis
    // deliveries follow it, even if the channel stays continuously busy.
    return this.queue.runTask(() => {
      const pending = this.starting.get(subscriber);
      this.starting.delete(subscriber);
      if (pending.length) {
        return this.applySnapshotWork(subscriber, async () => {
          for (const apply of pending) await apply();
        });
      }
    });
  }

  async applySnapshotWork(subscriber, apply) {
    let release;
    const finished = new Promise((resolve) => {
      release = resolve;
    });
    this.activeSnapshot = { subscriber, finished };
    try {
      await apply();
    } finally {
      this.activeSnapshot = null;
      release();
    }
  }

  /**
   * @param redisSubscriber
   * @param readInitial
   */
  async attach(redisSubscriber, readInitial) {
    this.starting.set(redisSubscriber, []);
    const acknowledgements = [];
    try {
      await this.queue.runTask(() => {
        for (const channel of redisSubscriber.channels) {
          if (!this.store[channel]) this.initializeChannel(channel);

          this.store[channel].push(redisSubscriber);
          acknowledgements.push(this.channelHandlers[channel].subscribed);
        }
      });
      // Neither the broker wait nor the Mongo read holds up existing observers.
      await Promise.all(acknowledgements);
      await readInitial();
      await this.finishInitialization(redisSubscriber);
    } catch (error) {
      this.starting.delete(redisSubscriber);
      this.detach(redisSubscriber);
      throw error;
    }
  }

  /**
   * @param redisSubscriber
   */
  detach(redisSubscriber) {
    this.queue.queueTask(() => {
      _.each(redisSubscriber.channels, (channel) => {
        if (!this.store[channel]) {
          return debug(
            "[RedisSubscriptionManager] Trying to detach a subscriber on a non existent channels."
          );
        } else {
          this.store[channel] = _.without(this.store[channel], redisSubscriber);

          if (this.store[channel].length === 0) {
            this.destroyChannel(channel);
          }
        }
      });
    });
  }

  /**
   * @param channel
   */
  initializeChannel(channel) {
    debug(`[RedisSubscriptionManager] Subscribing to channel: ${channel}`);

    // create the handler for this channel
    const self = this;
    const handler = function (message) {
      self.queue.queueTask(async () => {
        await self.process(channel, message, true);
      });
    };

    this.store[channel] = [];

    const { pubSubManager } = Config;
    this.channelHandlers[channel] = {
      handler,
      subscribed: pubSubManager.subscribe(channel, handler),
    };
  }

  /**
   * @param channel
   */
  destroyChannel(channel) {
    debug(`[RedisSubscriptionManager] Unsubscribing from channel: ${channel}`);

    const { pubSubManager } = Config;
    pubSubManager.unsubscribe(channel, this.channelHandlers[channel].handler);

    delete this.store[channel];
    delete this.channelHandlers[channel];
  }

  /**
   * @param channel
   * @param data
   * @param [fromRedis=false]
   */
  async process(channel, data, fromRedis) {
    // messages from redis that contain our uid were handled
    // optimistically, so we can drop them.
    if (fromRedis && data[RedisPipe.UID] === this.uid) {
      return;
    }

    // Optimistic writes bypass the listener queue. Wait before reading the
    // document so the caller still awaits its own observer updates.
    while (
      !fromRedis &&
      this.activeSnapshot &&
      this.store[channel]?.includes(this.activeSnapshot.subscriber)
    ) {
      await this.activeSnapshot.finished;
    }
    let subscribers = this.store[channel];
    if (!subscribers) {
      return;
    }

    debug(
      `[RedisSubscriptionManager] Received ${
        data[RedisPipe.SYNTHETIC] ? "synthetic " : ""
      }event: "${data[RedisPipe.EVENT]}" to "${channel}"`
    );

    if (this.starting.size) {
      subscribers = subscribers.filter((subscriber) => {
        const pending = this.starting.get(subscriber);
        if (!pending) return true;
        pending.push(() => this.deliver([subscriber], data));
        return false;
      });
    }
    if (subscribers.length) await this.deliver(subscribers, data);
  }

  async deliver(subscribers, data) {
    if (!data[RedisPipe.SYNTHETIC]) {
      const collection = subscribers[0].observableCollection.collection;

      let doc;
      if (data[RedisPipe.EVENT] === Events.REMOVE) {
        doc = data[RedisPipe.DOC];
      } else {
        doc = await this.getDoc(collection, subscribers, data);
      }

      // if by any chance it was deleted after it got dispatched
      // doc will be undefined
      if (!doc) {
        return;
      }

      for (const redisSubscriber of subscribers) {
        try {
          await redisSubscriber.process(
            data[RedisPipe.EVENT],
            doc,
            data[RedisPipe.FIELDS]
          );
        } catch (e) {
          debug(
            `[RedisSubscriptionManager] Exception while processing event: ${e.toString()}`
          );
        }
      }
    } else {
      for (const redisSubscriber of subscribers) {
        try {
          await redisSubscriber.processSynthetic(
            data[RedisPipe.EVENT],
            data[RedisPipe.DOC],
            data[RedisPipe.MODIFIER],
            data[RedisPipe.MODIFIED_TOP_LEVEL_FIELDS]
          );
        } catch (e) {
          debug(
            `[RedisSubscriptionManager] Exception while processing synthetic event: ${e.toString()}`
          );
        }
      }
    }
  }

  /**
   * @param collection
   * @param subscribers
   * @param data
   */
  async getDoc(collection, subscribers, data) {
    let doc = data[RedisPipe.DOC];

    if (
      collection._redisOplog &&
      !collection._redisOplog.protectAgainstRaceConditions
    ) {
      // If there's no protection against race conditions
      // It means we have received the full doc in doc

      return doc;
    }

    const fieldsOfInterest = getFieldsOfInterestFromAll(subscribers);

    if (fieldsOfInterest === true) {
      doc = await collection.findOneAsync(doc._id);
    } else {
      doc = await collection.findOneAsync(doc._id, {
        fields: fieldsOfInterest,
      });
    }

    return doc;
  }
}

export default new RedisSubscriptionManager();
