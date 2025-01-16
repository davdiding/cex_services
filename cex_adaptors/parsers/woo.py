from .base import Parser


class WOOParser(Parser):
    INTERVAL_MAP = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "12h": "12h",
        "1d": "1d",
        "1w": "1w",
        "1M": "1M",
    }

    def __init__(self) -> None:
        super().__init__()

    def check_response(self, response: dict) -> dict:
        # successfull response
        if response["success"]:
            return {"code": 200, "status": "success", "data": response["rows"]}

        else:
            return {"code": 400, "status": "error", "message": response}

    @staticmethod
    def _parse_created_timestamp(timestamp: str) -> int:
        if timestamp:
            return int(float(timestamp) * 1000)

    def parse_exchange_info(self, response: dict) -> dict:
        response = self.check_response(response)

        datas = response["data"]

        results = {}
        for data in datas:
            ii = self.parse_raw_symbol(data["symbol"])
            result = {
                "active": data["status"] == "TRADING",
                "is_spot": self.parse_is_spot(ii["market_type"]),
                "is_margin": self.parse_is_margin(ii["market_type"]),
                "is_futures": self.parse_is_futures(ii["market_type"]),
                "is_perp": self.parse_is_perpetual(ii["market_type"]),
                "is_linear": True,
                "is_inverse": False,
                "symbol": self.parse_unified_symbol(base=ii["base"], quote=ii["quote"]),
                "base": ii["base"],
                "quote": ii["quote"],
                "settle": ii["settle"],
                "multiplier": ii["multiplier"],
                "leverage": self.parse_str(data["base_asset_multiplier"], float),
                "listing_time": self._parse_created_timestamp(data["created_time"]),
                "expiration_time": None,  # API do not provide this information
                "contract_size": 1,
                "tick_size": self.parse_str(data["quote_tick"], float),
                "min_order_size": self.parse_str(data["quote_max"], float),
                "max_order_size": self.parse_str(data["quote_min"], float),
                "raw_data": data,
            }
            instrument_id = self.parse_unified_id(result)
            results[instrument_id] = result

        return results

    def parse_raw_symbol(self, symbol: str) -> dict:
        ss = symbol.split("_")

        return {
            "market_type": ss[0],
            "base": self.parse_base_currency(ss[1]),
            "quote": ss[2],
            "settle": ss[2],
            "multiplier": self.parse_multiplier(ss[1]),
        }

    def get_interval(self, interval: str) -> str:
        if interval not in self.INTERVAL_MAP:
            raise ValueError(f"Invalid interval: {interval}")
        return self.INTERVAL_MAP[interval]
