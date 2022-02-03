import secrets
import time
import sys
from typing import Optional, Type
from types import TracebackType

import anyio
import anyio.lowlevel
from typing_extensions import Self

from .conn import RedisConnection
from .resp import RedisException

__all__ = ('Redlock', 'RedlockManager')


RAND_VAL_BYTES = 20


# KEYS: (the lock key,)
# ARGV: (Expected random value,)
DELETE_EQUAL = b"""
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


# KEYS: (The lock key,)
# ARGV: (Expected random value, new expiration)
EXTEND_EQUAL = b"""
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""


class LockAcquisitionException(RuntimeError):
    ...


class Redlock:

    _expires_at: Optional[float]

    __slots__ = (
        '_manager', '_expires_at', '_val', 'key', 'timeout', 'delay', 'retries'
    )

    def __init__(
        self,
        manager: 'RedlockManager',
        key: str,
        *,
        timeout: int,
        delay: float,
        retries: Optional[int] = None
    ) -> None:
        self._manager = manager

        self._expires_at = None
        self._val = None

        self.key = key

        self.timeout = timeout
        self.delay = delay
        self.retries = retries

    async def __aenter__(self) -> Self:
        await self.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self.release()

    @property
    def acquired(self) -> bool:
        if self._expires_at is None:
            return False

        return self._expires_at > time.perf_counter()

    async def acquire_once(self) -> bool:
        self._val = secrets.token_bytes(RAND_VAL_BYTES)

        res = await self._manager._conn.command(
            'SET', self.key, self._val,
            'PX', self.timeout, 'NX'
        )

        if res == 'OK':
            self._expires_at = time.perf_counter() + self.timeout
            return True

        return False

    async def acquire(self) -> None:
        tries = 0
        max_tries = self.retries or float('inf')
        while tries < max_tries:
            if await self.acquire_once():
                return

            tries += 1
            await anyio.sleep(self.delay)
        else:
            raise LockAcquisitionException()

    async def release(self) -> bool:
        if self._expires_at is None or self._val is None:
            raise RuntimeError("Cannot release not yet acquired redlock")

        if self._expires_at - time.perf_counter() < 0:
            return False

        with anyio.move_on_after(self._expires_at - time.perf_counter(), shield=True):
            await self._manager._delete_equal(self.key, self._val)
            return True

        await anyio.lowlevel.checkpoint_if_cancelled()
        return False

    async def extend(self, ms: int) -> bool:
        if (
            self._expires_at is None or self._val is None
            or self._expires_at - time.perf_counter() < 0
        ):
            raise RuntimeError("Cannot release not currently acquired redlock")

        with anyio.fail_after(self._expires_at - time.perf_counter(), shield=True):
            await self._manager._extend_equal(self.key, self._val, ms)
            self._expires_at = time.perf_counter() + (ms / 1000)
            return True

        await anyio.lowlevel.checkpoint_if_cancelled()
        return False


class RedlockManager:
    def __init__(
        self,
        conn: RedisConnection,
        *,
        timeout: int,
        delay: float,
        retries: Optional[int] = None
    ) -> None:
        self._conn = conn

        self.timeout = timeout
        self.delay = delay
        self.retries = retries

    async def __aenter__(self) -> Self:
        await self._conn.__aenter__()
        try:
            await self._load_scripts()
        except:
            await self._conn.__aexit__(*sys.exc_info())
            raise

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        await self._conn.__aexit__(exc_type, exc_val, exc_tb)

    async def _load_scripts(self) -> None:
        self._del_equal_hash = await self._conn.command('SCRIPT LOAD', DELETE_EQUAL)
        self._extend_equal_hash = await self._conn.command('SCRIPT LOAD', EXTEND_EQUAL)

    async def _delete_equal(self, key: str, value: bytes) -> bool:
        try:
            success = self._conn.command('EVALSHA', self._del_equal_hash, 1, key, value)
        except RedisException as error:
            if not error.args[0].startswith('NOSCRIPT'):
                raise

            await self._load_scripts()
            success = self._conn.command('EVALSHA', self._del_equal_hash, 1, key, value)

        return success != 0

    async def _extend_equal(self, key: str, value: bytes, ms: int) -> bool:
        try:
            success = self._conn.command('EVALSHA', self._extend_equal_hash, 1, key, value, ms)
        except RedisException as error:
            if not error.args[0].startswith('NOSCRIPT'):
                raise

            await self._load_scripts()
            success = self._conn.command('EVALSHA', self._extend_equal_hash, 1, key, value, ms)

        return success != 0

    def acquire(self, key: str) -> Redlock:
        """Create a redlock instance from a key to acquire it.

        This method doesn't actually acquire the lock until you enter the
        Redlock or use the `acquire` method.

        Examples:

            ```python
            async with RedlockManager(DualLockedRedisConnection(...)) as conn:
                async with conn.acquire('ABC123') as lock:
                    ...
            ```

        Parameters:
            key: The key of the Redlock.

        Returns:
            A Redlock instance from the key, which can be entered as an
            asynchronous context manager.
        """
        return Redlock(
            self, key, timeout=self.timeout, delay=self.delay,
            retries=self.retries
        )
