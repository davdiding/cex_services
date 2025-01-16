from .base import BaseClient


class WOOUnified(BaseClient):
    name = "woo"
    BASE_ENDPOINT = "https://api.woo.org"
    PUB_ENDPOINT = "https://api-pub.woo.org"

    def __init__(self):
        super().__init__()
        self.base_endpoint = self.BASE_ENDPOINT
        self.pub_endpoint = self.PUB_ENDPOINT

    async def _get_available_symbols(self):
        return await self._get(self.base_endpoint + "/v1/public/info")

    async def _get_historical_klines(self, symbol: str, type: str, start_time: int) -> dict:
        params = {
            "symbol": symbol,
            "type": type,
            "start_time": start_time,
        }
        return await self._get(self.pub_endpoint + "/v1/hist/kline", params=params)

    async def _get_klines(self, symbol: str, type: str, limit: int) -> dict:
        params = {
            "symbol": symbol,
            "type": type,
            "limit": limit,
        }
        return await self._get(self.base_endpoint + "/v1/public/kline", params=params)
