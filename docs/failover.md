## Listener events

Configure listener callbacks through `Config.redisExtras.events`. The package supports `connect`, `reconnecting`, `error` and `end`.

`connect` runs after node-redis reports ready and Redis Oplog has finished reconciling active observers from MongoDB. With the default node-redis 3 resubscription behavior, the broker has acknowledged the remembered subscriptions at that point. This callback can run later than in 3.0.1. A socket that disconnects before reaching ready does not produce a package connect callback. For the underlying socket event, attach a listener to `getRedisListener().on("connect", handler)` after package initialization.

```js
import { Config } from 'meteor/cultofcoders:redis-oplog';

Object.assign(Config.redisExtras.events, {
    reconnecting({ delay, attempt }) {
        console.log('Redis listener retry', attempt, delay);
    },
    connect() {
        console.log('Redis listener ready and observers refreshed');
    },
});
```

## Observer startup

An observer that opens a new Redis channel waits for its SUBSCRIBE acknowledgement before reading MongoDB. An unavailable broker can therefore delay subscription readiness, and a rejected subscription command fails the observation. Equivalent queries share startup. Notifications received during the initial read are applied in order as the observer becomes live.

Redis Pub/Sub has no replay. Reconciliation can restore changes persisted in MongoDB. A synthetic notification sent while the listener was offline can be lost. Keep automatic resubscription enabled for this recovery behavior.

## Notes

Since we have the logic of refreshing an observable collection, we can make it so it falls back to polling.
But polling is very dangerous and very expensive, so we should avoid it at all costs, however
this could be implemented and customized per collection, but honestly, your redis-server needs to have
high-availability, since it's a core component of your app.
