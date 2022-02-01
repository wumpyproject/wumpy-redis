from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Sequence, Tuple, Union

import anyio
import anyio.abc
import anyio.lowlevel
import anyio.streams.tls
from anyio.streams.buffered import BufferedByteReceiveStream
from typing_extensions import Self

from .resp import SERIALIZABLE, receive_data, send_data, serialize_data

__all__ = (
    'RedisConnection', 'QueuedRedisConnection', 'DualLockedRedisConnection'
)


REDIS_PORT = 6379


class ConnectionClosed(RuntimeError):
    """Internal exception to mark a closed connection pair."""
    pass


class ConnectionPair:
    def __init__(self, origin: anyio.abc.ByteStream) -> None:
        self._closed = False

        self._send = origin
        self._receive = BufferedByteReceiveStream(origin)

    @property
    def send(self) -> anyio.abc.ByteSendStream:
        if self._closed:
            raise ConnectionClosed()

        return self._send

    @property
    def receive(self) -> BufferedByteReceiveStream:
        if self._closed:
            raise ConnectionClosed()

        return self._receive

    async def aclose(self) -> None:
        return await self._send.aclose()


class RedisConnection(ABC):
    """Base class for redis connections.

    This class simply wraps an underlying TCP connection to Redis. See
    subclasses for implementations of the sending and pipelining methods.
    """

    url: str
    tls: bool
    auth: Union[Tuple[str], Tuple[str, str], None]

    __slots__ = ('_conn', 'url', 'tls', 'auth')

    def __init__(
        self,
        url: str,
        *,
        tls: bool = True,
        auth: Union[str, Tuple[str], Tuple[str, str], None] = None
    ) -> None:
        self._conn = None

        self.url = url
        self.tls = tls
        self.auth = (auth,) if isinstance(auth, str) else auth

    async def __aenter__(self) -> Self:
        try:
            await self.reconnect()
        except:
            await self.__aexit__()
            raise

        return self

    async def __aexit__(self) -> None:
        if self._conn is not None:
            await self._conn.aclose()

    async def reconnect(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.aclose()
            finally:
                # Even if aclose() gets cancelled it'll forcefully disconnect
                # meaning that the socket has closed either way
                self._conn = None

        self._conn = ConnectionPair(await anyio.connect_tcp(
            self.url, REDIS_PORT, tls=self.tls
        ))

        if self.auth is not None:
            await self.command('AUTH', *self.auth)

    @abstractmethod
    async def command(self, *args: SERIALIZABLE) -> Any:
        """Send a command to the Redis server.

        Parameters:
            args:
                The arguments to send to the server, should start with the
                command as a string followed by its arguments. The only types
                supported are strings, integers, bytes, None and an iterable of
                the previous types.

        Returns:
            The response for the command.
        """
        ...

    @abstractmethod
    async def pipeline(self, commands: Sequence[Iterable[SERIALIZABLE]]) -> List[Any]:
        """Pipeline multiple commands at the same time.

        This is an optimization step available with Redis.

        Parameters:
            commands:
                A list of commands (see `command()` above) to send over the
                connection.

        Returns:
            A list of responses to the commands.
        """
        ...


class QueuedRedisConnection(RedisConnection):
    """Safe Redis implementaton for non-idempotent commands.

    This class uses one lock for both sending and receiving, queuing up all
    commands. That means that if one command fails or gets cancelled then only
    that command needs to retry but the downside becomes that a command cannot
    be sent until the previous one has received a response.

    Use this class if you need reliable failure (only the in-progress command
    gets retried on failure) and have less concurrent access or don't mind if
    it gets queued up unnecessarily.
    """

    __slots__ = ('_command_lock',)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._command_lock = anyio.Lock()

    async def command(self, *args: SERIALIZABLE) -> Any:
        """Send a command to the Redis server.

        Parameters:
            args:
                The arguments to send to the server, should start with the
                command as a string followed by its arguments. The only types
                supported are strings, integers, bytes, None and an iterable of
                the previous types.

        Returns:
            The response for the command.
        """
        async with self._command_lock:
            while True:
                if self._conn is None:
                    await self.reconnect()

                assert self._conn is not None, 'Reconnected Redis but there is no connection'
                conn = self._conn

                await send_data(conn.send, *args)

                try:
                    return await receive_data(conn.receive)
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
                except anyio.get_cancelled_exc_class():
                    # If we get cancelled waiting for a response then it will
                    # be received by the next task which would be detrimental.
                    # We'll need to reconnect the socket and discard the data
                    await self.reconnect()
                    raise

    async def pipeline(self, commands: Sequence[Iterable[SERIALIZABLE]]) -> List[Any]:
        """Pipeline multiple commands at the same time.

        This is an optimization step available with Redis.

        This method will retry commands until they succeed. For example if 5
        commands are sent but Redis cuts the connection after receiving 3 that
        means that the last two commands will be retried. Therefor your
        commands should be as idempotent as possible.

        Parameters:
            commands:
                A list of commands (see `command()` above) to send over the
                connection.

        Returns:
            A list of responses to the commands.
        """
        data = []

        while len(commands) - len(data) > 0:
            if self._conn is None:
                await self.reconnect()

            assert self._conn is not None, 'Reconnected Redis but there is no connection'
            conn = self._conn

            buffer = bytearray()

            for cmd in commands[len(data):]:
                serialize_data(*cmd, buffer=buffer)

            await conn.send.send(buffer)

            try:
                for _ in range(len(commands) - len(data)):
                    data.append(await receive_data(conn.receive))
            except ConnectionClosed:
                # While a task was waiting on the lock, a previous task
                # reconnected below. We need to retry the command.                        
                continue
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
            except anyio.get_cancelled_exc_class():
                # If we get cancelled waiting for a response then it will be
                # received by the next task which would be detrimental. We'll
                # need to reconnect the socket and discard this data..
                await self.reconnect()
                raise

        return data


class DualLockedRedisConnection(RedisConnection):
    """Redis connection based on two locks maximizing concurrent access.

    This implementation uses separate read, and write locks - allowing one
    task to write a command when the other is waiting for a response. The
    downside with the two locks is that all in-progress commands must be
    retried if one fails. Because of the increased amount of commands retried
    this leads a higher risk of commands being executed twice by Redis.

    Use this class if you need maximized concurrent access, on mostly
    idempotent commands. Afterall, it is only one worst-case scenarios that
    commands need to get retried.
    """

    __slots__ = ('_send_lock', '_receive_lock')

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._send_lock = anyio.Lock()
        self._receive_lock = anyio.Lock()

    async def command(self, *args: SERIALIZABLE) -> Any:
        """Send a command to the Redis server.

        All commands are assumed to be idempotent, as they are retried if the
        connection to Redis fails half-way through.

        This method should *not* be cancelled, doing so forces all other
        commands currently waiting to retry. This could lead to commands being
        executed twice by Redis.

        Parameters:
            args:
                The arguments to send to the server, should start with the
                command as a string followed by its arguments. The only types
                supported are strings, integers, bytes, None and an iterable of
                the previous types.

        Returns:
            The response for the command.
        """
        while True:
            if self._conn is None:
                await self.reconnect()

            assert self._conn is not None, 'Reconnected Redis but there is no connection'
            conn = self._conn

            async with self._send_lock:
                try:
                    await send_data(conn.send, *args)
                except ConnectionClosed:
                    continue

            # It is super important that nothing is awaited between exiting the
            # above lock and entering the next one!
            try:
                async with self._receive_lock:
                    try:
                        return await receive_data(conn.receive)
                    except ConnectionClosed:
                        # While a task was waiting on the lock, a previous task
                        # reconnected below. We need to retry the command.                        
                        continue
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
            except anyio.get_cancelled_exc_class():
                # If we get cancelled waiting for a response then it will be
                # received by the next task which would be detrimental. We'll
                # need to reconnect the socket and discard this data..
                await self.reconnect()
                raise

    async def pipeline(self, commands: Sequence[Iterable[SERIALIZABLE]]) -> List[Any]:
        """Pipeline multiple commands at the same time.

        This is an optimization step available with Redis.

        This method will retry commands until they succeed. For example if 5
        commands are sent but Redis cuts the connection after receiving 3 that
        means that the last two commands will be retried. Therefor your
        commands should be as idempotent as possible.

        This method should *not* be cancelled, doing so forces all other
        commands currently waiting to retry. This could lead to commands being
        executed twice by Redis. Additionally, it'll discard the data for
        commands already received.

        Parameters:
            commands:
                A list of commands (see `command()` above) to send over the
                connection.

        Returns:
            A list of responses to the commands.
        """
        data = []

        while len(commands) - len(data) > 0:
            if self._conn is None:
                await self.reconnect()

            assert self._conn is not None, 'Reconnected Redis but there is no connection'
            conn = self._conn

            buffer = bytearray()

            for cmd in commands[len(data):]:
                serialize_data(*cmd, buffer=buffer)

            async with self._send_lock:
                try:
                    await conn.send.send(buffer)
                except ConnectionClosed:
                    continue

            # It is super important that nothing is awaited between exiting the
            # above lock and entering the next one!

            try:
                async with self._receive_lock:
                    try:
                        for _ in range(len(commands) - len(data)):
                            data.append(await receive_data(conn.receive))
                    except ConnectionClosed:
                        # While a task was waiting on the lock, a previous task
                        # reconnected below. We need to retry the command.                        
                        continue
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
            except anyio.get_cancelled_exc_class():
                # If we get cancelled waiting for a response then it will be
                # received by the next task which would be detrimental. We'll
                # need to reconnect the socket and discard this data..
                await self.reconnect()
                raise

        return data
