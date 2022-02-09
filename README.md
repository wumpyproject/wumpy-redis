# Wumpy-redis

Optional subpackage with Redis utils for the Wumpy project.

## Contents

The subpackage (will) contain the following utilities:

- `RedisGateway`:
    A two-part replacement of the `Gateway` class in `wumpy-gateway`, using
    Redis lists to distribute commands. The point is to allow you to roll out
    updates easier, as you only need to restart the consumers of the list
    (which does not hold a gateway connection).

    Refer to the docstring for in-depth usage.

- `RedisLimiter`:
    A Redis-based implementation of the ratelimiter interface for `wumpy-rest`.
    Allowing shared locks between shards in different processes.

- `RedisGateway`:
    A two-part replacement of the `Gateway` class in `wumpy-rest` for use in
    `wumpy-client`, using Redis as a pub-sub. This makes rolling out updates
    easier, as you are no longer confined to the IDENTIFY limit.

    The way it works is by

- `RedisIdentifyLocker`:
    A lock-keeper for `wumpy-gateway` that uses Redis to ensure the ratelimit
    on the IDENTIFY command is respected.
