from .exchanges.woo import WOOUnified
from .parsers.woo import WOOParser


class WOO(WOOUnified, WOOParser):
    def __init__(self):
        super().__init__()
        self.exchange_info = {}

    async def sync_exchange_info(self):
        self.exchange_info = await self.get_exchange_info()

    async def get_exchange_info(self) -> dict:
        self.exchange_info = self.parse_exchange_info(await self._get_available_symbols())

        return self.exchange_info

    async def get_history_candlesticks(
        self, instrument_id: str, interval: str, start: int = None, end: int = None, num: int = None
    ) -> list:
        if instrument_id not in self.exchange_info:
            raise ValueError(f"{instrument_id} not found in exchange info")

        info = self.exchange_info[instrument_id]
        _symbol = self.get_interval(interval)
        _interval = info["raw_data"]["symbol"]
        return await self._get_klines(_interval, _symbol, num)
