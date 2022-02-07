import secrets
import sys
import time
from types import TracebackType
from typing import Optional, Type

import anyio
import anyio.lowlevel
from typing_extensions import Self

from .conn import RedisConnection
from .resp import RedisException

__all__ = ('Redlock', 'RedlockManager')


RAND_VAL_BYTES = 20


# KEYS: (The lock key,)
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
    """Exception indicating that the lock could not be acquired.

    This exception is only raised when the maximum retries passed to create the
    `Redlock` is hit and the lock is still not acquired. Consider adjusting the
    amount of retries or delay between each attempt.
    """
    pass


class Redlock:
    """Redis-based spinlock implementation."""

    _manager: 'RedlockManager'

    _expires_at: Optional[float]
    _val: Optional[bytes]

    key: str
    timeout: int
    delay: float
    retries: Optional[int]

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
        """Whether the lock is still acquired.

        The point of this property is to calculate whether the lock has expired
        or not by comparing timestamps.
        """
        if self._expires_at is None:
            return False

        return self._expires_at > time.perf_counter()

    async def acquire_once(self) -> bool:
        """Attempt to acquire the lock once.

        This method does not account for `retries` or `delay`; all it does is
        attempt to acquire the lock once and return a bool indicating whether
        the lock is now owned by the current task.

        Returns:
            Whether the lock was acquired by the current task.
        """
        self._val = secrets.token_bytes(RAND_VAL_BYTES)

        res = await self._manager._conn.command(
            'SET', self.key, self._val,
            'PX', str(self.timeout), 'NX'
        )

        if res == 'OK':
            self._expires_at = time.perf_counter() + self.timeout
            return True

        return False

    async def acquire(self) -> None:
        """Continuously attempt to acquire a lock until it succeeds.

        Compared to `acquire_once()` this method *does* respect `retries` and
        `delay` - calling `acquire_once()` in a loop until it works.

        Raises:
            LockAcquisitionException:
                The lock was not successfully acquired after `retries` tries.
        """
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
        """Attempt to release the lock.

        If the lock has already expired this method will do nothing and return
        `False` immediately.

        Raises:
            RuntimeError: This method was called before the lock was acquired.

        Returns:
            Whether the lock was released successfully. This can be `False` if
            the lock expired and got acquired by another lock.
        """
        if self._expires_at is None or self._val is None:
            raise RuntimeError("Cannot release not yet acquired redlock")

        try:
            if not self.acquired:
                return False

            with anyio.move_on_after(self._expires_at - time.perf_counter(), shield=True):
                return await self._manager._delete_equal(self.key, self._val)
        finally:
            self._val = None
            self._expires_at = None

        # If we got cancelled while shielding we should give the event loop a
        # chance to raise the exception again.
        await anyio.lowlevel.checkpoint_if_cancelled()
        return False

    async def extend(self, ms: int) -> None:
        """Extend the lifetime of the lock.

        This method must not be called after the lock has expired and
        potentially acquired by another task.

        Raises:
            RuntimeError: The lock was not acquired when this was called.
            RuntimeError: Another task acquired the lock.
        """
        if self._expires_at is None or self._val is None or not self.acquire:
            raise RuntimeError("Cannot release not currently acquired redlock")

        if not await self._manager._extend_equal(self.key, self._val, ms):
            raise RuntimeError('Failed to extend lock as it was acquired by another task')

        self._expires_at = time.perf_counter() + (ms / 1000)


class RedlockManager:
    """Lock manager over Redlocks.

    This class houses Redis-based spinlocks and should be alive for as long as
    all locks are. Initialize it with a connection (that has not been started
    yet) and configuration for each redlock, then use it as an asynchronous
    context manager.

    The core functionality is in the `acquire()` method.

    Examples:

        ```python
        async with RedlockManager(QueuedRedisConnection(), timeout=6000, delay=0.05):
            ...
        ```
    """

    _conn: RedisConnection

    _del_equal_hash: Optional[str]
    _extend_equal_hash: Optional[str]

    timeout: int
    delay: float
    retries: Optional[int]

    def __init__(
        self,
        conn: RedisConnection,
        *,
        timeout: int,
        delay: float,
        retries: Optional[int] = None
    ) -> None:
        self._conn = conn

        self._del_equal_hash = None
        self._extend_equal_hash = None

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
        self._del_equal_hash = await self._conn.command('SCRIPT', 'LOAD', DELETE_EQUAL)
        self._extend_equal_hash = await self._conn.command('SCRIPT', 'LOAD', EXTEND_EQUAL)

    async def _delete_equal(self, key: str, value: bytes) -> bool:
        try:
            success = await self._conn.command(
                'EVALSHA', self._del_equal_hash, '1', key, value
            )
        except RedisException as error:
            if not error.args[0].startswith('NOSCRIPT'):
                raise

            await self._load_scripts()
            success = await self._conn.command(
                'EVALSHA', self._del_equal_hash, '1', key, value
            )

        return success != 0

    async def _extend_equal(self, key: str, value: bytes, ms: int) -> bool:
        try:
            success = await self._conn.command(
                'EVALSHA', self._extend_equal_hash, '1', key, value, str(ms)
            )
        except RedisException as error:
            if not error.args[0].startswith('NOSCRIPT'):
                raise

            await self._load_scripts()
            success = await self._conn.command(
                'EVALSHA', self._extend_equal_hash, '1', key, value, ms
            )

        return success != 0

    def acquire(self, key: str) -> Redlock:
        """Create a redlock instance from a key to acquire it.

        This method doesn't actually acquire the lock until you enter the
        Redlock or use the `acquire` method.

        Examples:

            ```python
            async with RedlockManager(DualLockedRedisConnection(...), ...) as conn:
                async with conn.acquire('ABC123') as lock:
                    ...
            ```

            ```python
            async with RedlockManager(DualLockedRedisConnection(...), ...) as conn:
                lock = conn.acquire('ABC123')
                await lock.acquire()
                try:
                    ...
                finally:
                    await lock.release()
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
