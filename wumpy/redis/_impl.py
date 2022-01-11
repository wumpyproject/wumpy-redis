"""AnyIO implementation of RESP and a Redis connection."""
from contextlib import asynccontextmanager
from typing import Any, Iterable, Literal, Optional, Union, Tuple

import anyio
import anyio.streams.tls
from typing_extensions import Self


REDIS_PORT = 6379

SERIALIZABLE = Union[int, str, bytes, Iterable['SERIALIZABLE']]


def serialize(*args: SERIALIZABLE, buffer: Optional[bytearray] = None) -> bytes:
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
        elif isinstance(obj, (int, bool)):
            buffer += b':%d\r\n' % obj
        elif isinstance(obj, bytes):
            buffer += b'$%d\r\n%s\r\n' % (len(obj), obj)
        else:
            serialize(*obj, buffer=buffer)

    return buffer


def _value(buffer: bytearray):
    l = buffer.index(b'\r\n')

    # Skip the type prefix, as well as the termination characters
    data = buffer[1:l-1]
    del buffer[:l + 2]  # len(b'\r\n')

    return data


SERIALIZE_TYPES = {
    ord('+'): str,
    ord(':'): int,
    ord('$'): lambda d: d if d != b'-1' else None,
    ord('*'): lambda d: [SERIALIZE_TYPES[d[0]](_value(d)) for _ in range(int(d))]
}


def deserialize(buffer: bytearray) -> Any:
    """Recursively deserialize a buffer.

    The data will be consumed from the buffer, leaving any unused data left.

    Parameters:
        buffer: The bytearray buffer of data.

    Returns:
        The deserialized data.
    """
    if buffer[0] == ord('*'):
        items = SERIALIZE_TYPES[ord(':')](_value(buffer))

        return [deserialize(buffer) for _ in range(items)]

    return SERIALIZE_TYPES[buffer[0]](_value(buffer))


class RedisConnection:
    """Connection to Redis."""

    _sock: Optional[anyio.streams.tls.TLSStream]

    url: str
    tls: bool
    auth: Union[Tuple[str], Tuple[str, str], None]

    def __init__(self, url: str, *, tls: bool = True, auth: Union[str, Tuple[str], Tuple[str, str], None] = None) -> None:
        self._buffer = bytearray()
        self._sock = None

        self.url = url
        self.tls = tls
        self.auth = (auth,) if isinstance(auth, str) else auth

    async def __aenter__(self) -> Self:
        self._sock = await anyio.connect_tcp(
            self.url, REDIS_PORT, tls=self.tls
        )

        if self.auth is not None:
            await self._sock.send(serialize('AUTH', *self.auth))

        return self

    async def __aexit__(self) -> None:
        if self._sock is None:
            return

        await self._sock.aclose()

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

        await self._sock.send(serialize(*args))

    async def receive(self) -> Any:
        """Receive a response from Redis.

        Returns:
            The deserialized data received.
        """
        if self._sock is None:
            raise RuntimeError('Tried to receive a response before connecting to Redis')

        while True:
            while self._buffer[-2:] != '\n\r':
                self._buffer.extend(await self._sock.receive())

            try:
                return deserialize(self._buffer)
            except ValueError:
                continue
