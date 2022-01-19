"""AnyIO implementation of RESP and a Redis connection."""
from contextlib import asynccontextmanager
from typing import Any, Iterable, Literal, Optional, Union, Tuple

import anyio
import anyio.streams.tls
from typing_extensions import Self

from .resp import SERIALIZABLE, serialize, deserialize

__all__ = ('RedisConnection',)


REDIS_PORT = 6379


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
            try:
                await self._sock.send(serialize('AUTH', *self.auth))
            except:
                await self.__aexit__()
                raise

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
