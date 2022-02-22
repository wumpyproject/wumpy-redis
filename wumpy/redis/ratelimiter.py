from typing import Any, AsyncContextManager, Callable, Mapping, Optional

from wumpy.rest import Route


class RedisRatelimiterLock:
    """Lock for"""

    def __init__(self, limiter: 'RedisLimiter', route: Route) -> None:
        self.limiter = limiter
        self.route = route

    async def __aenter__(self) -> Callable[[Mapping[str, str]], Any]:
        self.bucket = self.limiter.command('GET', 'rl-bkts:' + self.route.endpoint)

        if self.bucket is None:
            # There are only two rare cases where this can happen:
            # - A cold start means Redis does not contain all buckets
            # - This endpoint has no ratelimit (only global applies)

            # Either way, our best bet is to leave it untracked, if this is a
            # cold start Redis will be updated once we receive the response.
            return self.update

        lock_held = '0'
        while lock_held == '0':
            try:
                await self.limiter.command('BLPOP', 'rl-bkts:GLOBAL')
            except RedisException:
                pass

            try:
                lock_held = await self.limiter.command('BLPOP', self.bucket + self.route.major_params)
            except RedisException:
                await self.limiter.command('LPUSH', self.bucket + self.route.major_params, '1')

        await self.limiter.command('EXPIRE', self.bucket + self.route.major_params, self.limiter.ttl)

        return self.update

    async def __aexit__(self) -> bool:
        lock = await self.limiter.command('GET', self.bucket + self.route.major_params)

        if len(lock) == 0:
            await self.limiter.command('LPUSH', self.bucket + self.route.major_params, True)

    async def update(self, headers: Mapping[str, str]) -> None:
        try:
            bucket = headers['X-RateLimit-Bucket']
        except KeyError:
            pass
        else:
            await self.limiter.command('SET', 'rlbs:' + self.route.endpoint, bucket)


class RedisLimiter:
    """Redis-based implementation of the ratelimiter interface.

    This class uses Redis for distributing locks, allowing for shared
    ratelimiting across processes during sharding.

    It is important to note that this may not actually be necessary for many
    setups because of the way ratelimiting works. Ratelimits are determined by
    the major parameters they affect - which is the guild's ID, channel's ID
    or webhook's ID - but they will always be different because of the way
    sharding is done on the guild ID.

    That said, the usage of this is required to respect ratelimits when a
    shard needs to use an endpoint for a guild or channel it is not receiving
    events from, because another shard may try to use the same endpoint.

    There are multiple (non-mutually exclusive) strategies that this class can
    take depending on configuration:

    2. Passing `ttl` as an integer, or None will change how long it takes until
       a lock is automatically released in the event of a failure. If `ttl` is
       an integer this is the Redis TTL the lock will have. It is very
       important to consider API response times from Discord when choosing this
       value, as to not prematurely release the lock. A request can take up to
       100 seconds before Cloudflare times out the response, but waiting this
       long in the event of an unreleased lock could be too long depending on
       setup and usage.

       If `ttl` is None, the actual Redis TTL will be dynamically calculated
       depending on response times. This is the recommended option, as it
       balances the potential of releasing the lock to early and a lock being
       held even though the task that acquired it has died.
    """

    def __init__(self, conn: RedisConnection, ttl: int = 30) -> None:
        self._conn = conn

        self.ttl = ttl

    async def __aenter__(self) -> Callable[[Route], AsyncContextManager[]]:
        await self._conn.__aenter__()
        return self.get

    async def __aexit__(self):
        await self._conn.__aenter__()

    def get(self, route: Route) -> RedisRatelimiterLock:
        return RedisRatelimiterLock(self, route)
