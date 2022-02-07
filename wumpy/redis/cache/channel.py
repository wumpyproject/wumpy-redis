import itertools

from ._base import RedisCacheBase


class RedisMessageCache(RedisCacheBase):

    MSG_KEY_FORMAT = 'messages/{}-{}'

    def __init__(self, *args, message_ttl: int, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.message_ttl = message_ttl

    async def _process_message_create(self, data) -> Message:
        d = data['d']

        await self._session.send(
            'HSET', self.MSG_KEY_FORMAT.format(d['id'], d['channel_id']),
            *itertools.chain.from_iterable(d.items())
        )
