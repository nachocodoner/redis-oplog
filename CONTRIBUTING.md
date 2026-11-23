# Contribute

## Introduction

First, thank you for considering contributing to redis-oplog! It's people like you that make the open source community such a great community! 😊

We welcome any type of contribution, not only code. You can help with

- **QA**: file bug reports, the more details you can give the better (e.g. screenshots with the console open)
- **Marketing**: writing blog posts, howto's, printing stickers, ...
- **Community**: presenting the project at meetups, organizing a dedicated meetup for the local community, ...
- **Code**: take a look at the [open issues](issues). Even if you can't write code, commenting on them, showing that you care about a given issue matters. It helps us triage them.
- **Money**: we welcome financial contributions in full transparency on our [open collective](https://opencollective.com/redis-oplog).

## Your First Contribution

Working on your first Pull Request? You can learn how from this _free_ series, [How to Contribute to an Open Source Project on GitHub](https://egghead.io/series/how-to-contribute-to-an-open-source-project-on-github).

## Submitting code

Any code change should be submitted as a pull request. The description should explain what the code does and give steps to execute it. The pull request should also contain tests.

## Code review process

The bigger the pull request, the longer it will take to review and merge. Try to break down large pull requests in smaller chunks that are easier to review and merge.
It is also always helpful to have some context for your pull request. What was the purpose? Why does it matter to you?

## Running Tests

### Setup

```
meteor create --release 3.1.2 --bare test
cd test
meteor npm i --save puppeteer@1.18.1 simpl-schema@3.4.6 chai@4.3.6
```

### Start Tests

Run the package suite from `test/` with a local Redis server listening on `127.0.0.1:6379`. Set `REDIS_OPLOG_TEST_PORT` to use a separate local server, or set `REDIS_OPLOG_TEST_URL` to pass a URL to both package connections. For an IPv6-only run, bind a disposable Redis to `::1` and use a URL such as `redis://[::1]:19675/0`.

```
METEOR_PACKAGE_DIRS="../" TEST_BROWSER_DRIVER=puppeteer meteor test-packages --raw-logs --once --driver-package meteortesting:mocha ../
```

For the two-process publication and reconnect regression, run the next command from the same `test/` directory on macOS or Linux with `redis-server` on PATH. It starts an isolated broker and MongoDB, writes through one Meteor process, and checks the browser subscribed through the other.

```sh
meteor node ../testing/cross-instance/run.cjs
```

The runner launches Chromium through the Puppeteer installation above. Each run starts its own Redis on a free loopback port and its own Meteor MongoDB. Only the reader’s Redis connection is interrupted. Logs and the temporary app are retained on failure.

The default run checks remote CRUD, inserts and removals during initial observation, shared startup and retry after a failed query, and repeated Redis listener interruptions. It also times an update on a separate collection while a recovery query is held for 250 ms. For the focused delete-during-recovery test use `--reload-overlap`, or use `--reload-control` to write after recovery.

The following commands run just the initial subscription or the repeated reconnect regressions from the same test app.

```sh
meteor node ../testing/cross-instance/run.cjs --initial-handoff
meteor node ../testing/cross-instance/run.cjs --pre-resubscribe
```

The first changes membership during the initial query. The second writes on a reconnected socket before Redis has restored the reader’s subscriptions. Each sends a later live marker before comparing the full projected Minimongo and MongoDB results. [Audit and findings](docs/reactivity-audit.md)

### Meteor 3.6.1 and core observer tests

For the 3.6.1 package and two-process suites, use the same bare test app setup with `--release 3.6.1`. The default two-process run also covers two publications that contribute different fields of one document to the same browser. If Mocha stops at “RUNNING CLIENT TESTS”, check that the Chromium executable exists using the following command. An incomplete extraction on macOS was resolved by reusing a complete copy of Puppeteer's pinned Chromium revision 672088.

```sh
meteor node -e "require('node:fs').accessSync(require('puppeteer').executablePath(), require('node:fs').constants.X_OK)"
```

From the 3.6.1 test app, run the paired upstream check with the path to a Meteor checkout containing the 3.6.1 release commit.

```sh
meteor node ../testing/cross-instance/run.cjs --core-parity /path/to/meteor
```

The runner extracts the release's Mongo package into a temporary directory and runs its `observeChanges -` Tinytests on the server and in Chromium. It runs native Meteor first, then repeats with this local Redis Oplog package and a disposable Redis instance. A separate assertion checks the active driver in each lane. The runner compares the case inventories, verifies compiled source hashes and requires all 16 server and 11 browser cases to pass in each lane. The printed failure directory contains the browser and server logs and, when both lanes finish, `core-results.json`. [Results](docs/reactivity-audit.md#meteor-361-observer-compatibility)

To repeat the Redis 8.12.0 checks, put its `src` or `bin` directory first on `PATH`. The package suite uses the Redis listening at the fixture's configured port. The two-process and core commands each start a private `redis-server` from that path. The 3.6.1 CI job builds the official archive, checks its SHA-256 and verifies the running server's version with `INFO server`. [Workflow](.github/workflows/test.yml) · [Upstream archive checksums](https://github.com/redis/redis-hashes)

## Financial contributions

We also welcome financial contributions in full transparency on our [open collective](https://opencollective.com/redis-oplog).
Anyone can file an expense. If the expense makes sense for the development of the community, it will be "merged" in the ledger of our open collective by the core contributors and the person who filed the expense will be reimbursed.

## Questions

If you have any questions, create an [issue](issue) (protip: do a quick search first to see if someone else didn't ask the same question before!).
You can also reach us at hello@redis-oplog.opencollective.com.

## Credits

### Contributors

Thank you to all the people who have already contributed to redis-oplog!
<a href="graphs/contributors"><img src="https://opencollective.com/redis-oplog/contributors.svg?width=890" /></a>

### Backers

Thank you to all our backers! [[Become a backer](https://opencollective.com/redis-oplog#backer)]

<a href="https://opencollective.com/redis-oplog#backers" target="_blank"><img src="https://opencollective.com/redis-oplog/backers.svg?width=890"></a>

### Sponsors

Thank you to all our sponsors! (please ask your company to also support this open source project by [becoming a sponsor](https://opencollective.com/redis-oplog#sponsor))

<a href="https://opencollective.com/redis-oplog/sponsor/0/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/0/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/1/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/1/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/2/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/2/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/3/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/3/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/4/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/4/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/5/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/5/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/6/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/6/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/7/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/7/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/8/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/8/avatar.svg"></a>
<a href="https://opencollective.com/redis-oplog/sponsor/9/website" target="_blank"><img src="https://opencollective.com/redis-oplog/sponsor/9/avatar.svg"></a>

<!-- This `CONTRIBUTING.md` is based on @nayafia's template https://github.com/nayafia/contributing-template -->
