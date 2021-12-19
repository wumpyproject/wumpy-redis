"""AnyIO implementation of RESP and a Redis connection."""
from contextlib import asynccontextmanager
from typing import Any, Iterable, Optional, Union

import anyio
from typing_extensions import Self


REDIS_PORT = 6379

SERIALIZABLE = Union[int, str, bytes, Iterable['SERIALIZABLE']]


class RedisConnection:
    """Connection to Redis."""

    SERIALIZE_TEMPLATES = {
        str: b'+%s\r\n',
        int: b':%d\r\n',
        bytes: b'$%d\r\n%s\r\n',
        None: b'$-1\r\n'
    }

    def __init__(self, url: str, *, tls: bool, auth: Union[Tuple[str], Tuple[str, str]]) -> None:
        self._sock = None
        self._command_lock = None

        self.url = url
        self.tls = tls
        self.auth = auth

    async def __aenter__(self) -> Self:
        self._sock = await anyio.connect_tcp(
            self.url, REDIS_PORT, tls=self.tls
        )

        # We need to delay the instantiation until there's an event loop
        # running so that AnyIO knows which one to use.
        self._command_lock = anyio.Lock()

        if self.auth is not None:
            await self._sock.send(self.serialize('AUTH', *self.auth))

        return self

    async def __aexit__(self) -> None:
        if self._sock is None:
            return

        await self._sock.aclose()

    def serialize(self, *args: SERIALIZABLE, buffer: Optional[bytearray] = None) -> bytes:
        """Serialize the arguments with RESP.

        This method only supports serializing an array, as that is the
        representation that commands take. There is no point in serializing
        anything else on its own.

        Because you can have nested arrays this method is implemented
        recursively - therefore it is up to you to ensure that you pass no
        cyclic iterable, otherwise RecursionError will be raised.

        Parameters:
            args: The arguments to serialize.
            buffer:
                The buffer to use, used when recurring but can be passed if you
                already have a buffer to use.

        Returns:
            The bytes representation of `args`.
        """

        buffer = buffer or bytearray()

        buffer += b'*%d\r\n' % len(args)

        for obj in args:
            if obj is None:
                buffer += b'$-1\r\n'
            elif isinstance(obj, str):
                buffer += b'+%s\r\n' % obj
            elif isinstance(obj, int):
                buffer += b':%d\r\n'
            elif isinstance(obj, bytes):
                buffer += b'$%d\r\n%s\r\n' % (len(obj), obj)
            else:
                self.serialize(*obj, buffer=buffer)

        return buffer

    def deserialize(self, data: bytearray) -> Any:
        """Deserialize the response from Redis.

        Raises:
            ValueError: The data
        Returns:
            The deserialized response from a command.
        """

    async def send(self, *args: SERIALIZABLE) -> None:
        """Send a command to the Redis server.

        Parameters:
            args:
                The arguments to send to the server, should start with the
                command as a string followed by its arguments. The only types
                supported are strings, integers, bytes, None and an iterable of
                the previous types.
            retry: Whether to retry the operation if the connection closed.

        Returns:
            The response for the command.
        """
        if self._sock is None:
            raise RuntimeError('Tried to send a command before connecting to Redis')

        await self._sock.send(self.serialize(*args))
