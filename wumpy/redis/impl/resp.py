from typing import Any, Iterable, Union

import anyio.abc
from anyio.streams.buffered import BufferedByteReceiveStream

__all__ = ('serialize_data', 'send_data', 'receive_data')


SERIALIZABLE = Union[None, int, str, bytes, Iterable['SERIALIZABLE']]
MAX_STR_SIZE = 512_000_000


class RedisException(Exception):
    """Exception wrapping an exception value returned by Redis."""
    pass


def serialize_data(*args, buffer: bytearray) -> None:
    buffer += b'*%d\r\n' % len(args)

    for obj in args:
        if obj is None:
            buffer += b'$-1\r\n'
        elif isinstance(obj, (str, bytes)):
            buffer += b'$%d\r\n%s\r\n' % (len(obj), obj)
        elif isinstance(obj, (int, bool)):
            buffer += b':%d\r\n' % obj
        else:
            serialize_data(*obj, buffer=buffer)


async def send_data(
    socket: anyio.abc.ByteSendStream,
    *args: SERIALIZABLE,
) -> None:
    """Serialize the arguments with RESP and send it over the socket.

    This method only supports serializing an array, as that is the
    representation that commands take. There is no point in serializing
    anything else on its own.

    Because you can have nested arrays this method is implemented
    recursively - therefore it is up to you to ensure that you pass no
    cyclic iterable, otherwise RecursionError will be raised.

    Parameters:
        socket: The socket to send the data over.
        args: The arguments to serialize.
        buffer:
            The buffer to use, used when recurring but can be passed if you
            already have a buffer to use.

    Returns:
        The bytes representation of `args`.
    """

    buffer = bytearray()

    serialize_data(*args, buffer=buffer)

    print('sending', buffer)
    return await socket.send(buffer)


async def receive_data(stream: BufferedByteReceiveStream) -> Any:
    """Receive one RESP value from the stream.

    The data will be consumed from the stream, the rest of the data will be
    left intact.

    Parameters:
        stream: The buffered receive stream.

    Raises:
        RedisException: Redis returned an exception.

    Returns:
        The deserialized data.
    """

    t = await stream.receive_exactly(1)
    if t == b'+':
        return (await stream.receive_until(b'\r\n', MAX_STR_SIZE + 2)).decode('utf-8')

    if t == b'-':
        d = (await stream.receive_until(b'\r\n', MAX_STR_SIZE + 2)).decode('utf-8')
        raise RedisException(d)

    if t == b':':
        return int((await stream.receive_until(b'\r\n', 19 + 2)))

    if t == b'$':
        
        l = int((await stream.receive_until(b'\r\n', 19 + 2)))
        if l == -1:
            return None

        return (await stream.receive_exactly(l + 2))[:-2]

    if t == b'*':
        l = int((await stream.receive_until(b'\r\n', 19 + 2)))
        return [await receive_data(stream) for _ in range(l)]

    raise ValueError(f'Unexpected type identifier: {t!r}')
