import { getRedisListener, getRedisPusher } from "./getRedisClient";

/**
 * Manages communication with Redis
 * Unifies all libraries that use this
 */
export default class PubSubManager {
  constructor() {
    this.channelHandlers = {};
    this.queue = new Meteor._AsynchronousQueue();

    this.listener = getRedisListener();
    this.pusher = getRedisPusher();

    this._initMessageListener();
  }

  /**
   * Pushes to Redis
   * @param {string} channel
   * @param {object} message
   */
  async publish(channel, message) {
    await this.pusher.publish(channel, EJSON.stringify(message));
  }

  /**
   * @param {string} channel
   * @param {function} handler
   */
  subscribe(channel, handler) {
    const subscribed = this.queue.runTask(async () => {
      if (this.channelHandlers[channel]) {
        this.channelHandlers[channel].push(handler);
        return;
      }

      // A message can arrive in the same socket read as the SUBSCRIBE reply.
      this.channelHandlers[channel] = [handler];
      try {
        await new Promise((resolve, reject) => {
          this.listener.subscribe(channel, (error) =>
            error ? reject(error) : resolve()
          );
        });
      } catch (error) {
        delete this.channelHandlers[channel];
        throw error;
      }
    });
    // Vent and existing public callers subscribe without awaiting a result.
    subscribed.catch((error) => Meteor._debug("Redis SUBSCRIBE failed", error));
    return subscribed;
  }

  /**
   * @param {string} channel
   * @param {function} handler
   */
  unsubscribe(channel, handler) {
    this.queue.queueTask(async () => {
      if (!this.channelHandlers[channel]) {
        return;
      }

      this.channelHandlers[channel] = this.channelHandlers[channel].filter(
        (_handler) => {
          return _handler !== handler;
        }
      );

      if (this.channelHandlers[channel].length === 0) {
        await this._destroyChannel(channel);
      }
    });
  }

  /**
   * Initializes listening for redis messages
   * @private
   */
  _initMessageListener() {
    const self = this;

    this.listener.on(
      "message",
      Meteor.bindEnvironment(async function (channel, _message) {
        if (self.channelHandlers[channel]) {
          const message = EJSON.parse(_message);
          for (const channelHandler of self.channelHandlers[channel]) {
            await channelHandler(message);
          }
        }
      })
    );
  }

  /**
   * @param channel
   * @private
   */
  async _destroyChannel(channel) {
    await this.listener.unsubscribe(channel);

    delete this.channelHandlers[channel];
  }
}
