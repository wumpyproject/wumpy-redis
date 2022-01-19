import secrets
from typing import Optional

import anyio
import anyio.lowlevel

from .conn import RedisConnection

__all__ = ('RedLock',)


RAND_VAL_BYTES = 20


class LockAcquisitionException(RuntimeError):
    ...


class RedLock:

    __slots__ = ('_conn', '_acquired', 'key', 'timeout', 'delay', 'retries')

    def __init__(
        self,
        conn: RedisConnection,
        key: str,
        *,
        timeout: int,
        delay: float,
        retries: Optional[int] = None
    ) -> None:
        self._conn = conn
        self._acquired = False

        self.key = key

        self.timeout = timeout
        self.delay = delay
        self.retries = retries

    @property
    def acquired(self) -> bool:
        return self._acquired

    async def acquire_once(self) -> bool:
        res = await self._conn.send(
            'SET', self.key, secrets.token_bytes(RAND_VAL_BYTES),
            'PX', self.timeout, 'NX'
        )

        if res == 'OK':
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

    async def release(self) -> None:
        with anyio.move_on_after(self.timeout, shield=True):
            ...

        await anyio.lowlevel.checkpoint_if_cancelled()
