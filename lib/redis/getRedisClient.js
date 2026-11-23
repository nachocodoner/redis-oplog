import redis from "redis";
import Config from "../config";
import { Meteor } from "meteor/meteor";

// Redis requires two connections for pushing and listening to data
let redisPusher;
let redisListener;

/**
 * Returns the pusher for events in Redis
 *
 * @returns {*}
 */
export function getRedisPusher() {
  if (!redisPusher) {
    redisPusher = redis.createClient(
      Object.assign({}, Config.redis, {
        retry_strategy: getRetryStrategy(),
      })
    );
  }

  return redisPusher;
}

/**
 * Returns the listener for events in Redis
 *
 * @param onConnect
 * @returns {*}
 */
export function getRedisListener({ onConnect } = {}) {
  if (!redisListener) {
    redisListener = redis.createClient(
      Object.assign({}, Config.redis, {
        retry_strategy: getRetryStrategy(),
      })
    );

    // we only attach events here
    attachEvents(redisListener, { onConnect });
  }

  return redisListener;
}

/**
 *
 * @param client
 * @param onConnect
 */
function attachEvents(client, { onConnect }) {
  const functions = ["connect", "reconnecting", "error", "end"];

  functions.forEach((fn) => {
    // node-redis 3 emits ready after the replies to its automatic SUBSCRIBEs.
    // Keep the configured connect hook after recovery of that connection.
    client.on(
      fn === "connect" ? "ready" : fn,
      Meteor.bindEnvironment(async function (...args) {
        if (fn === "connect" && onConnect) {
          await onConnect();
        }

        if (Config.redisExtras.events[fn]) {
          return Config.redisExtras.events[fn](...args);
        }
      })
    );
  });
}

/**
 * Retrieves the retry strategy that can be modified
 * @returns {Function}
 */
function getRetryStrategy() {
  return function (...args) {
    if (Config.redisExtras.retry_strategy) {
      return Config.redisExtras.retry_strategy(...args);
    }
  };
}
