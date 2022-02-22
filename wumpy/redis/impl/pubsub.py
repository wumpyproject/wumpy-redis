from collections import deque
from typing import Any

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream
from typing_extensions import Self

from .resp import receive_data

from .conn import QueuedRedisConnection

__all__ = ('RedisPubSubConnection',)


class RedisPubSubConnection(QueuedRedisConnection):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._msgs = deque()
        self._lock = anyio.Lock()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> str:
        if self._msgs:
            # We technically should have a checkpoint here because this
            # codepath does not yield to the event loop, the downside is that
            # it would make it very inefficient if you try to catch up by
            # filtering out most messages since this would yield each time.
            # The code would look like this:
            #     msg = self._msgs.popleft()
            #     await anyio.lowlevel.checkpoint()
            #     return msg
            return self._msgs.popleft()

        async with self._lock:
            if self._conn is None:
                await self.reconnect()

            assert self._conn is not None, 'Reconnected Redis but there is no connection'

            while True:
                try:
                    await self._receive(self._conn.receive)
                    if self._msgs:
                        return self._msgs.popleft()
                except (anyio.EndOfStream, anyio.IncompleteRead):
                    # The stream was closed before a complete response was
                    # received, best to just reconnect and try again.
                    await self.reconnect()
                    continue
                except (ValueError, anyio.DelimiterNotFound):
                    # Redis didn't respond with a valid RESP response.
                    # Something must be wrong with Redis so we should
                    # reconnect and try again.
                    await self.reconnect()
                    continue

    async def _receive(self, sock: BufferedByteReceiveStream) -> Any:
        while True:
            data = await receive_data(sock)
            if isinstance(data, list) and data[0] == 'message':
                self._msgs.append(data)

            return data
