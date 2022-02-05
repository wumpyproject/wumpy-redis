import base64
from typing import Any
from unittest import mock

import pytest
from anyio.streams.buffered import BufferedByteReceiveStream

from wumpy.redis.impl.resp import receive_data, send_data


@pytest.fixture
def anyio_backend():
    return 'asyncio'


class TestSendData:
    async def serialize(self, *args) -> bytes:
        m = mock.AsyncMock()
        await send_data(m, *args)
        return m.send.call_args.args[0]

    @pytest.mark.anyio
    async def test_string_ok(self) -> None:
        assert await self.serialize('OK') == b'*1\r\n$2\r\nOK\r\n'

    @pytest.mark.anyio
    async def test_string(self) -> None:
        s = '❤️ Thanks for taking the time to understand/read these tests 🙌'
        assert await self.serialize(s) == b'*1\r\n$62\r\n%s\r\n' % s.encode('utf-8')

    @pytest.mark.anyio
    async def test_integer(self) -> None:
        assert await self.serialize(123) == b'*1\r\n:123\r\n'

    @pytest.mark.anyio
    async def test_bool(self) -> None:
        assert await self.serialize(True) == b'*1\r\n:1\r\n'
        assert await self.serialize(False) == b'*1\r\n:0\r\n'

    @pytest.mark.anyio
    async def test_negative_integer(self) -> None:
        assert await self.serialize(-666) == b'*1\r\n:-666\r\n'

    @pytest.mark.anyio
    async def test_64bit_integer(self) -> None:
        # According to the RESP specification integers will be within the
        # 64-bit range, this is the maximum value within that range.
        assert (
            await self.serialize(9_223_372_036_854_775_807)
            == b'*1\r\n:9223372036854775807\r\n'
        )

    @pytest.mark.anyio
    async def test_bulk_string(self) -> None:
        # Yes, yes this is some random Among Us copypasta from Reddit with some
        # sprinkled in newlines and null characters. Bulk strings are supposed
        # to be binary-safe.
        s = (
            'Red 🔴\r 📛\n sus 💦 💦. Red 🔴 🔴\tsuuuus. I 👁👄 👁 said 🤠👱🏿💦 red 👹\r'
            '\n 🔴, sus 💦 💦, hahahahaha 🤣 🤣.\0 Why 🤔 arent you 👉😯 👈 laughing 😂?'
        ).encode('utf-8')
        assert await self.serialize(s) == b'*1\r\n$%d\r\n' % len(s) + s + b'\r\n'

    @pytest.mark.anyio
    async def test_none(self) -> None:
        assert await self.serialize(None) == b'*1\r\n$-1\r\n'

    @pytest.mark.anyio
    async def test_array(self) -> None:
        assert await self.serialize(1, 2, 3) == b'*3\r\n:1\r\n:2\r\n:3\r\n'

    @pytest.mark.anyio
    async def test_nested_array(self) -> None:
        assert await self.serialize(True, [['OK']]) == b'*2\r\n:1\r\n*1\r\n*1\r\n$2\r\nOK\r\n'

    @pytest.mark.anyio
    async def test_array_bulk_string(self) -> None:
        assert (
            await self.serialize(b'OK', b'MAYBE', [b'NO', b'NOPE'])
            == b'*3\r\n$2\r\nOK\r\n$5\r\nMAYBE\r\n*2\r\n$2\r\nNO\r\n$4\r\nNOPE\r\n'
        )


class TestReceiveData:
    async def receive(self, data: bytes) -> Any:
        m = mock.AsyncMock()
        m.receive.return_value = data
        stream = BufferedByteReceiveStream(m)
        data = await receive_data(stream)
        assert stream.buffer == b''
        return data

    @pytest.mark.anyio
    async def test_simple_ok(self) -> None:
        assert await self.receive(b'+OK\r\n') == 'OK'

    @pytest.mark.anyio
    async def test_simple_string(self) -> None:
        # Base64 is a binary-safe format which leads to a pretty good test-case
        # considering the encoded string will use a complicated combination of
        # 64 different characters in the test.
        s = '❤️ Thanks for taking the time to understand/read these tests 🙌'.encode('utf-8')
        assert (
            await self.receive(b'+' + base64.b64encode(s) + b'\r\n')
            == base64.b64encode(s).decode('utf-8')
        )

    @pytest.mark.anyio
    async def test_integer(self) -> None:
        assert await self.receive(b':123\r\n') == 123

    @pytest.mark.anyio
    async def test_negative_integer(self) -> None:
        assert await self.receive(b':-666\r\n') == -666

    @pytest.mark.anyio
    async def test_64bit_integer(self) -> None:
        # According to the RESP specification integers will be within the
        # 64-bit range, this is the maximum value within that range.
        assert (
            await self.receive(b':9223372036854775807\r\n')
            == 9_223_372_036_854_775_807
        )

    @pytest.mark.anyio
    async def test_bulk_string(self) -> None:
        # Yes, yes this is some random Among Us copypasta from Reddit with some
        # sprinkled in newlines and null characters. Bulk strings are supposed
        # to be binary-safe.
        s = (
            'Red 🔴\r 📛\n sus 💦 💦. Red 🔴 🔴\tsuuuus. I 👁👄 👁 said 🤠👱🏿💦 red 👹\r'
            '\n 🔴, sus 💦 💦, hahahahaha 🤣 🤣.\0 Why 🤔 arent you 👉😯 👈 laughing 😂?'
        ).encode('utf-8')
        assert await self.receive(b'$%d\r\n' % len(s) + s + b'\r\n') == s

    @pytest.mark.anyio
    async def test_none(self) -> None:
        assert await self.receive(b'$-1\r\n') == None

    @pytest.mark.anyio
    async def test_array(self) -> None:
        assert await self.receive(b'*3\r\n:1\r\n:2\r\n:3\r\n') == [1, 2, 3]

    @pytest.mark.anyio
    async def test_nested_array(self) -> None:
        assert await self.receive(b'*2\r\n:1\r\n*1\r\n*1\r\n+OK\r\n') == [1, [['OK']]]

    @pytest.mark.anyio
    async def test_array_bulk_string(self) -> None:
        assert await self.receive(
            b'*3\r\n$2\r\nOK\r\n$5\r\nMAYBE\r\n*2\r\n$2\r\nNO\r\n$4\r\nNOPE\r\n'
        ) == [b'OK', b'MAYBE', [b'NO', b'NOPE']]
