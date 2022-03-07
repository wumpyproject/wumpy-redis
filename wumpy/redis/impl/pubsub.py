import collections
import sys
from types import TracebackType
from typing import Any, NoReturn, Optional, Type

import anyio
import anyio.abc
import anyio.lowlevel
from anyio.streams.buffered import BufferedByteReceiveStream
from typing_extensions import Self

from .conn import QueuedRedisConnection
from .resp import receive_data

__all__ = ('RedisPubSubConnection',)


class Outcome:

    __slots__ = ('_manager', '_event', 'value',)

    def __init__(self) -> None:
        self._event = anyio.Event()

    def set(self, value: object) -> None:
        self.value = value

        self._event.set()

    async def wait(self) -> Any:
        # Note that there is a race condition where the event is given a value
        # and cancelled, because of the usage this is currently fine because
        # even though a command won't have any chance to do anything with the
        # value.
        await self._event.wait()

        if not hasattr(self, 'value'):
            raise RuntimeError('Outcome woken up without value')

        return self.value


class OutcomeManager:
    def __init__(self) -> None:
        self._waiters = collections.deque()
        self._values = collections.deque()

    def send(self, value: object) -> None:
        if self._waiters:
            wait = self._waiters.popleft()
            wait.set(value)
        else:
            self._values.append(value)

    async def receive(self) -> Any:
        await anyio.lowlevel.checkpoint_if_cancelled()

        if self._values:
            val = self._values.popleft()
            try:
                await anyio.lowlevel.cancel_shielded_checkpoint()
            except BaseException:
                self._values.appendleft(val)
                raise

            return val

        outcome = Outcome()
        self._waiters.append(outcome)
        return await outcome.wait()


class RedisPubSubConnection(QueuedRedisConnection):

    _tasks: anyio.abc.TaskGroup

    _msgs: OutcomeManager
    _cmds: OutcomeManager

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._msgs = OutcomeManager()
        self._cmds = OutcomeManager()

    def __aiter__(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        self._tasks = await anyio.create_task_group().__aenter__()

        try:
            await super().__aenter__()
        except:
            await self._tasks.__aexit__(*sys.exc_info())
            raise

        self._tasks.start_soon(self._consumer_task)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> Optional[bool]:
        # Cancel _consumer_task() currently infinitely running
        self._tasks.cancel_scope.cancel()
        try:
            return await self._tasks.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            await super().__aexit__(exc_type, exc_val, exc_tb)

    async def _consumer_task(self) -> NoReturn:
        if self._conn is None:
            raise RuntimeError('Cannot start the consumer task before being connected')

        while True:
            try:
                data = await receive_data(self._conn.receive)
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

            if isinstance(data, list):
                if data[0] == 'message':
                    self._msgs.send(data[1])
                else:
                    self._cmds.send(data[1])
            else:
                self._cmds.send(data)

    async def __anext__(self) -> str:
        return await self._msgs.receive()

    async def _receive(self, sock: BufferedByteReceiveStream) -> Any:
        return await self._cmds.receive()
