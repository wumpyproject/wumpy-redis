import base64

from wumpy.redis.impl import serialize


class TestSerializer:
    def test_simple_ok(self) -> None:
        assert serialize('OK') == b'*1\r\n+OK\r\n'

    def test_simple_string(self) -> None:
        # Base64 is a binary-safe format which leads to a pretty good test-case
        # considering the encoded string will use a complicated combination of
        # 64 different characters in the test.
        s = '❤️ Thanks for taking the time to understand/read these tests 🙌'.encode('utf-8')
        assert serialize(base64.b64encode(s).decode('utf-8')) == b'*1\r\n+%s\r\n' % base64.b64encode(s)

    def test_integer(self) -> None:
        assert serialize(123) == b'*1\r\n:123\r\n'

    def test_bool(self) -> None:
        assert serialize(True) == b'*1\r\n:1\r\n'
        assert serialize(False) == b'*1\r\n:0\r\n'

    def test_negative_integer(self) -> None:
        assert serialize(-666) == b'*1\r\n:-666\r\n'

    def test_64bit_integer(self) -> None:
        # According to the RESP specification integers will be within the
        # 64-bit range, this is the maximum value within that range.
        assert serialize(9_223_372_036_854_775_807) == b'*1\r\n:9223372036854775807\r\n'

    def test_bulk_string(self) -> None:
        # Yes, yes this is some random Among Us copypasta from Reddit with some
        # sprinkled in newlines and null characters. Bulk strings are supposed
        # to be binary-safe.
        s = (
            'Red 🔴\r 📛\n sus 💦 💦. Red 🔴 🔴\tsuuuus. I 👁👄 👁 said 🤠👱🏿💦 red 👹\r'
            '\n 🔴, sus 💦 💦, hahahahaha 🤣 🤣.\0 Why 🤔 arent you 👉😯 👈 laughing 😂?'
        ).encode('utf-8')
        assert serialize(s) == b'*1\r\n$%d\r\n' % len(s) + s + b'\r\n'

    def test_none(self) -> None:
        assert serialize(None) == b'*1\r\n$-1\r\n'

    def test_array(self) -> None:
        assert serialize(1, 2, 3) == b'*3\r\n:1\r\n:2\r\n:3\r\n'

    def test_nested_array(self) -> None:
        assert serialize(True, [['OK']]) == b'*2\r\n:1\r\n*1\r\n*1\r\n+OK\r\n'

    def test_array_bulk_string(self) -> None:
        assert serialize(
            b'OK', b'MAYBE', [b'NO', b'NOPE']
        ) == b'*3\r\n$2\r\nOK\r\n$5\r\nMAYBE\r\n*2\r\n$2\r\nNO\r\n$4\r\nNOPE\r\n'
