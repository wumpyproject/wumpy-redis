from typing import Any, AsyncContextManager, Callable, Optional, Mapping

from wumpy.rest import Route


class RedisLock:
    """Lock for"""

    def __init__(self, conn: 'RedisLimiter', route: Route) -> None:
        self.conn = conn
        self.route = route

    async def __aenter__(self) -> Callable[[Mapping[str, str]], Any]:
        self.conn

    async def __aexit__(self) -> bool:
        ...


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

    1. Passing `primitive=True` will make the strategy provide less safety.
       Specifically, this relies on Redis' own asynchronous replication to
       ensure that the lock is acquired across all masters. The reason it
       provides less safety is because a master could go down before
       replicating the key, and for other masters the lock stays released.

       This is a perfectly fine failure for some cases, because of how rare it
       should be, as well as the worst-case scenario only being a 429 response
       from Discord.

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

    def __init__(self, url: str, ttl: Optional[int] = None) -> None:
        self._conn = ...

    async def __aenter__(self) -> Callable[[Route], AsyncContextManager[]]:
        ...

    async def __aexit__(self) -> bool:
        ...
