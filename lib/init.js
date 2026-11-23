// https://github.com/luin/ioredis#connect-to-redis
import Config from "./config";
import extendMongoCollection from "./mongo/extendMongoCollection";
import RedisSubscriptionManager from "./redis/RedisSubscriptionManager";
import PubSubManager from "./redis/PubSubManager";
import { getRedisListener } from "./redis/getRedisClient";
import deepExtend from "deep-extend";

let isInitialized = false;

export default (config = {}) => {
  if (isInitialized) {
    throw "You cannot initialize RedisOplog twice.";
  }

  isInitialized = true;

  deepExtend(Config, config);

  Object.assign(Config, {
    isInitialized: true,
    oldPublish: Meteor.publish,
  });

  extendMongoCollection();

  // this initializes the listener singleton with the proper onConnect functionality
  getRedisListener({
    onConnect: () => RedisSubscriptionManager.reloadSubscribers(),
  });

  RedisSubscriptionManager.init();
  Config.pubSubManager = new PubSubManager();
};
