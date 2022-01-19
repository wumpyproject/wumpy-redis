from typing import Optional, Union, Iterable, Any

__all__ = ('serialize', 'deserialize')


SERIALIZABLE = Union[None, int, str, bytes, Iterable['SERIALIZABLE']]


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
            buffer += b'+%s\r\n' % obj.encode('utf-8')
        elif isinstance(obj, (int, bool)):
            buffer += b':%d\r\n' % obj
        elif isinstance(obj, bytes):
            buffer += b'$%d\r\n%s\r\n' % (len(obj), obj)
        else:
            serialize(*obj, buffer=buffer)

    return bytes(buffer)


class Scanner:
    """Safe interface to a mutable bytearray.

    This is used instead of directly using the bytearray, because it only
    consumes the bytes once completed. That way, if deserializing fails because
    the whole payload was not present the bytearray is left untouched.

    Attributes:
        buffer: The bytearray buffer used.
    """
    __slots__ = ('_stopped', 'buffer')

    def __init__(self, buffer: bytearray) -> None:
        self._stopped = 0
        self.buffer = buffer

    def read(self, stop: Union[bytes, int]) -> bytearray:
        start = self._stopped

        if isinstance(stop, int):
            self._stopped = stop
            return self.buffer[start:stop]

        i = self.buffer.index(stop, self._stopped)
        self._stopped = i

        return self.buffer[start:i]

    def consume(self) -> bytearray:
        del self.buffer[0:self._stopped]
        return self.buffer

def _value(buffer: memoryview) -> tuple[memoryview, memoryview]:
    l = buffer.index(b'\r\n')

    # Skip the type prefix, as well as the termination characters
    data = buffer[1:l-1]
    rest = buffer[:l + 2]  # len(b'\r\n')

    return data, rest


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
