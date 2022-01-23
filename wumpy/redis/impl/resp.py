from multiprocessing.sharedctypes import Value
from typing import Optional, Union, Iterable, Any

import anyio.abc
from anyio.streams.buffered import BufferedByteReceiveStream

__all__ = ('send_data', 'receive_data')


SERIALIZABLE = Union[None, int, str, bytes, Iterable['SERIALIZABLE']]
MAX_STR_SIZE = 512_000_000


class RedisException(Exception):
    """Exception wrapping an exception value returned by Redis."""
    pass


def _serialize_data(*args, buffer: bytearray) -> None:
    buffer += b'*%d\r\n' % len(args)

    for obj in args:
        if obj is None:
            buffer += b'$-1\r\n'
        elif isinstance(obj, str):
            buffer += b'+%s\r\n' % obj.encode('utf-8')
        elif isinstance(obj, (int, bool)):
            buffer += b':%d\r\n' % obj
        elif isinstance(obj, bytes):
            buffer += b'$%d\r\n%s\r\n' % (len(obj), obj)
        else:
            _serialize_data(*obj, buffer=buffer)


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

    _serialize_data(*args, buffer=buffer)

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
        return str((await stream.receive_until(b'\r\n', MAX_STR_SIZE + 2))[:-1])

    if t == b'-':
        d = str((await stream.receive_until(b'\r\n', MAX_STR_SIZE + 2))[:-1])
        raise RedisException(d)

    if t == ord(':'):
        return int((await stream.receive_until(b'\r\n', 19 + 2))[:-1])

    if t == ord('$'):
        l = int((await stream.receive_until(b'\r\n', 19 + 2))[:-1])
        s = (await stream.receive_exactly(l + 2))[:-1]
        if s == b'-1':
            return None
        return s

    if t == ord('*'):
        l = int((await stream.receive_until(b'\r\n', 19 + 2))[:-1])
        return [receive_data(stream) for _ in range(l)]

    raise ValueError(f'Unexpected type identifier: {t!r}')
