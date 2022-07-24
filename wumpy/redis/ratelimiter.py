import json
import math
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from types import TracebackType
from typing import (
    AsyncContextManager, AsyncGenerator, Awaitable, Callable, Mapping, NoReturn, Optional,
    Tuple, Type, Union
)

import anyio
from wumpy.rest import Route

from .impl import (
    REDIS_PORT, DualLockedRedisConnection, RedisPubSubConnection, Redlock,
    RedlockManager
)

__all__ = ('RedisRatelimiter',)


GLOBAL_RL_CHANNEl = 'wumpy:rl-global'


class RedisRatelimiterLock:

    def __init__(self, limiter: 'RedisRatelimiter', route: Route) -> None:
        self._limiter = limiter
        self._deferred = False

        self.lock = None
        self.route = route

    @asynccontextmanager
    async def acquire(self):
        bucket = await self._limiter.get_bucket(self.route)
        if bucket is None:
            self.bucket = self.route.endpoint
        else:
            self.bucket = bucket.decode('utf-8')

        self.lock = self._limiter.get_redlock(
            'wumpy:rl:' + self.bucket + self.route.major_params
        )

        await self.lock.acquire()
        try:
            await self._limiter.wait()
            yield self.update
        finally:
            if not self._deferred:
                await self.lock.release()

    async def update(self, headers: Mapping[str, str]) -> None:
        if self.lock is None:
            raise RuntimeError("'update()' method called before acquiring")

        try:
            bucket = headers['X-RateLimit-Bucket']
        except KeyError:
            pass
        else:
            if not self.bucket == bucket:
                await self._limiter.update_bucket(self.route, bucket)

        try:
            reset = float(headers['X-RateLimit-Remaining'])
        except (KeyError, ValueError):
            return  # We can't run the rest of the code without this info

        reset = datetime.fromtimestamp(reset, timezone.utc)

        try:
            remaining = int(headers['X-RateLimit-Remaining'])
        except (KeyError, ValueError):
            pass
        else:
            if remaining == 0:
                delta = reset - datetime.now(timezone.utc)
                await self.lock.extend(math.ceil(delta.total_seconds() * 1000))

                # This is done afterwards, because we only want to skip a
                # release if it was purposefully extended. If the above failed
                # with an exception then it will propagate to the acquire and
                # we still want it to release.
                self._deferred = True


class RedisRatelimiter:
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
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: int,
        delay: int,
        port: int = REDIS_PORT,
        tls: bool = True,
        auth: Union[str, Tuple[str], Tuple[str, str], None] = None
    ) -> None:
        self._conn = DualLockedRedisConnection(url, port=port, tls=tls, auth=auth)
        self._locks = RedlockManager(self._conn, timeout=timeout, delay=delay)
        self._pubsub = RedisPubSubConnection(url, port=port, tls=tls, auth=auth)

        self._stack = AsyncExitStack()

        self._global_rl = anyio.Event()
        self._global_rl.set()

    async def __aenter__(self) -> Callable[
        [Route], AsyncContextManager[
            Callable[[Mapping[str, str]], Awaitable[object]]
        ]
    ]:
        try:
            await self._stack.enter_async_context(self._conn)
            await self._stack.enter_async_context(self._locks)

            await self._stack.enter_async_context(self._pubsub)
            await self._pubsub.command('SUBSCRIBE', GLOBAL_RL_CHANNEl)

            self._tasks = await self._stack.enter_async_context(anyio.create_task_group())
            self._tasks.start_soon(self._consume_pubsub)
        except BaseException:
            await self._stack.__aexit__(*sys.exc_info())
            raise

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        self._tasks.cancel_scope.cancel()
        await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    @asynccontextmanager
    async def __call__(self, route: Route) -> AsyncGenerator[
        Callable[[Mapping[str, str]], Awaitable[object]], None 
    ]:
        async with RedisRatelimiterLock(self, route).acquire() as update:
            yield update

    async def _consume_pubsub(self) -> NoReturn:  # type: ignore
        async for message in self._pubsub:
            try:
                data = json.loads(message)
            except ValueError:
                continue

            try:
                if data['locked']:
                    self.lock()
                    await anyio.sleep(data['duration'])

                    self.unlock()
                else:
                    self.unlock()
            except KeyError:
                continue

    async def get_bucket(self, route: Route) -> Optional[bytes]:
        return await self._conn.command('GET', 'wumpy:rl-buckets:' + route.endpoint)

    async def update_bucket(self, route: Route, bucket: str) -> None:
        await self._conn.command('SET', 'wumpy:rl-buckets:' + route.endpoint, bucket)

    def get_redlock(self, key: str) -> Redlock:
        return self._locks.acquire(key)

    def lock(self) -> None:
        if self._global_rl.is_set():
            self._global_rl = anyio.Event()

    def unlock(self) -> None:
        self._global_rl.set()

    async def wait(self) -> None:
        await self._global_rl.wait()
