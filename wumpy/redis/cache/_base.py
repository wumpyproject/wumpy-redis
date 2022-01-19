from typing import Optional

from typing_extensions import Self, TypeAlias

from ..impl.conn import RedisConnection

__all__ = ('RedisCacheBase',)


class RedisCacheBase:
    """The base for a Redis-based cache."""

    _session: RedisConnection

    __slots__ = ('_session', 'url', 'tls', 'auth')

    def __init__(
        self,
        url: str,
        *args,
        tls: bool = True,
        auth: Union[Tuple[str], Tuple[str, str], None] = None,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self._session = RedisConnection(url, tls=tls, auth=auth)

    async def __aenter__(self) -> Self:
        self._session = await self._session.__aenter__()
        return self

    async def __aexit__(self, *exc_info) -> None:
        return await self._session.__aexit__(*exc_info)

    async def update(self, event: Dict[str, Any]) -> Any:
        processor = getattr(self, f'_process_{event["t"].lower()}', None)
        if processor is None:
            return None
        return await processor(event)
