import json
import sys
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any, AsyncGenerator, Mapping, Optional, Type, Union

from typing_extensions import Self

from .impl import RedisConnection, RedlockManager

try:
    from wumpy.gateway import DefaultGatewayLimiter  # type: ignore
except ImportError:
    class DefaultGatewayLimiter:
        def __init__(self, *args, **kwargs) -> None:
            raise ImportError(
                "wumpy-gateway is a required dependency to use 'RedisMaxConcurrencyLimiter'"
            )


__all__ = ('RedisGateway',)


class RedisGateway:
    """Redis' `BLPOP`-based gateway implementation.

    The gateway is split up into two parts; using Redis for distributed tasks
    over one shard. The benefit of this is allowing for seamless updates with
    no downtime for reconnecting a shard.

    This class implements both functionalities - it can both push and receive
    events - used by the gateway, although it is not recommended to do both at
    the same time.

    **To push events** simply use this class as an asynchronous context manager
    and forward all events received by the gateway to `push()`.

    **To receive events** use this class as an asynchronous context manager and
    then as an async iterator - which yields each event popped from the list.

    Because of how Redis lists and the `BLPOP` command works, it is guaranteed
    that each event received is unique and that no other task will receive the
    same event. Although Discord guarantees are still relevant, and Discord
    provides no guarantees on events only being received once.
    """

    def __init__(self, conn: RedisConnection, *, key: str = 'wumpy:gateway:events') -> None:
        self._conn = conn

        self.key = key

    def __aiter__(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        await self._conn.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self._conn.__aexit__(exc_type, exc_val, exc_tb)

    async def __anext__(self) -> Mapping[str, Any]:
        return await self.receive_event()

    async def receive_event(self) -> Mapping[str, Any]:
        data = await self._conn.command('BLPOP', self.key, '0')
        if len(data) == 2:
            return self.decode(data[1])

        raise RuntimeError("BLPOP command unexpectedly timed out")

    async def push(self, data: Mapping[str, Any]) -> None:
        """Push data to the Redis list.

        This should be called for each command received over the gateway so
        that they are processed by another task. It is not recommended to both
        consume and push to the list, as that defeats its purpose.

        Parameters:
            data: The data received by the gateway to push to the list.
        """
        await self._conn.command('LPUSH', self.key, self.encode(data))

    def encode(self, payload: Mapping[str, Any]) -> Union[str, bytes]:
        """Endcode the payload to a string or bytes.

        By default this is implemented as `json.dumps()` because it is part of
        the standard library, **but it is recommended that you override with a
        better implementation**. The reason this doesn't use a better encoding
        is because of dependencies.

        Parameters:
            payload: The dictionary payload received by `push()`.

        Returns:
            The string or bytes to append to the Redis list.
        """
        return json.dumps(payload)

    def decode(self, data: bytes) -> Mapping[str, Any]:
        """Decode the bytes to a payload.

        This should do the opposite of `encode()`.

        Similarly, this is implemented as `json.loads` and **is recommended to
        be overwritten with a better implementation**.

        Parameters:
            data: The bytes data popped from the Redis list.

        Returns:
            The decoded payload that should be returned to the user.
        """
        return json.loads(data)


class RedisMaxConcurrencyLimiter(DefaultGatewayLimiter):
    """Redis gateway limiter following max concurrency.

    Parameters:
        manager:
            An instance of `RedlockManager`, `timeout` has to be set to more
            than 5 seconds. This will be used to acquire a lock and then let it
            expire automatically.
        bucket:
            Max concurrency bucket of the current connection. This is
            calculated by `shard_id % max_concurrency` - defaults to 0 because
            the majority of bots have `max_concurrency == 1`.
        key:
            The key to format and use for the redlock. This has to have one
            `{}` that gets formatted with the `bucket` parameter.
    """
    def __init__(
        self,
        manager: RedlockManager,
        *,
        bucket: int = 0,
        key: str = 'wumpy:gateway:max-concurrency:{}',
    ) -> None:
        super().__init__()

        if manager.timeout < 5000:
            raise ValueError(
                'RelockManager has to be configured with a timeout equal to, '
                'or greater than 5 seconds'
            )

        self._locks = manager

        self.key = key.format(bucket)

    @asynccontextmanager
    async def __call__(self, opcode: int) -> AsyncGenerator[None, None]:
        if opcode == 2:
            # This will acquire the lock for 'timeout' amount of time, which
            # should be at least 5 seconds. We don't release the lock because
            # that would require creating a task for it or similar.
            await self._locks.acquire(self.key)

        async with super().__call__(opcode):
            yield

    async def __aenter__(self) -> Self:
        await self._locks.__aenter__()
        try:
            return await super().__aenter__()
        except:
            await self._locks.__aexit__(*sys.exc_info())
            raise

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self._locks.__aexit__(exc_type, exc_val, exc_tb)
