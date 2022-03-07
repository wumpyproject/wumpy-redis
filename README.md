# Wumpy-redis

Optional subpackage with Redis utils for the Wumpy project.

## Contents

The subpackage contains the following utilities:

- `RedisRatelimiter`:
    A Redis-based implementation of the ratelimiter interface for `wumpy-rest`.
    Allowing shared locks between shards in different processes.

- `RedisGateway`:
    A two-part replacement of the `Gateway` class in `wumpy-rest` for use in
    `wumpy-bot`, using Redis as a pub-sub. This makes rolling out updates
    easier, as you are no longer confined to the IDENTIFY limit.

- `RedisMaxConcurrencyLimiter`:
    A lock-keeper for `wumpy-gateway` that uses Redis to ensure the
    max-concurrency on the IDENTIFY command is respected.
