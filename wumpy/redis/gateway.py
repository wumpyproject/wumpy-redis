from ._impl import RedisConnection


class RedisGateway:
    def __init__(self, channel: str = 'gateway') -> None:
        self.channel = channel

    async def publish(self, event:)