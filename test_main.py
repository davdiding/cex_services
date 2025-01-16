import os
from datetime import datetime as dt
from datetime import timedelta as td
from unittest import IsolatedAsyncioTestCase

import pandas as pd
from dotenv import load_dotenv

from cex_adaptors.binance import Binance
from cex_adaptors.bitget import Bitget
from cex_adaptors.bybit import Bybit
from cex_adaptors.gateio import Gateio
from cex_adaptors.htx import Htx
from cex_adaptors.kucoin import Kucoin
from cex_adaptors.okx import Okx
from cex_adaptors.woo import WOO

load_dotenv()


def get_yesterday_timestamp():
    date = dt.date(dt.now()) - td(days=1)
    return int(dt(date.year, date.month, date.day).timestamp() * 1000)


class TestOkx(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.exchange = Okx(
            api_key=os.getenv("OKX_API_KEY_DEMO"),
            api_secret=os.getenv("OKX_API_SECRET_DEMO"),
            passphrase=os.getenv("OKX_API_PASSPHRASE_DEMO"),
            flag="1",
        )
        await self.exchange.sync_exchange_info()

    async def asyncTearDown(self):
        await self.exchange.close()

    async def test_exchange_info(self):
        spot = await self.exchange.get_exchange_info("spot")
        self.assertTrue(spot is not None)

        futures = await self.exchange.get_exchange_info("futures")
        self.assertTrue(futures is not None)

        perp = await self.exchange.get_exchange_info("perp")
        self.assertTrue(perp is not None)

        exchange_info = await self.exchange.get_exchange_info()
        self.assertTrue(exchange_info is not None)
        return

    # Private endpoints
    async def test_get_account_info(self):
        account = await self.exchange.get_account_info()
        self.assertTrue(account)
        return

    async def test_get_balance(self):
        balance = await self.exchange.get_balance()
        self.assertTrue(balance)
        return

    async def test_get_positions(self):
        position = await self.exchange.get_positions()
        self.assertTrue(position)
        return

    async def test_place_market_order(self):
        instrument_id = "ADA/USDT:USDT"
        side = "buy"
        volume = 1000
        order = await self.exchange.place_market_order(instrument_id, side, volume)
        self.assertTrue(order)
        return

    async def test_place_limit_order(self):
        instrument_id = "ADA/USDT:USDT"
        side = "buy"
        price = 0.5
        volume = 1000
        order = await self.exchange.place_limit_order(instrument_id, side, price, volume)

        cancel = await self.exchange.cancel_order(instrument_id, order["order_id"])
        self.assertTrue(cancel)
        return

    async def test_get_funding_rate_with_timestamp(self):
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.exchange.exchange_info if self.exchange.exchange_info[k]["is_futures"]][0]

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        for i in [perp, futures]:
            funding_rate = await self.exchange.get_history_funding_rate(i, start=start, end=end)
            self.assertEqual(len(funding_rate), target_num, f"{i} {len(funding_rate)}")

        return

    async def test_get_current_candlesticks(self):
        instrument_id = "BTC/USDT:USDT"
        kline = await self.exchange.get_current_candlestick(instrument_id, "1d")
        self.assertTrue(kline)
        return

    async def test_get_history_candlesticks(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"

        num = 678
        interval = "1d"

        for i in [spot, perp]:
            kline = await self.exchange.get_history_candlesticks(i, interval, num=num)
            self.assertEqual(len(kline), num, f"{i} {len(kline)}")
        return


class TestBinance(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.binance = Binance(
            api_key=os.getenv("BINANCE_API_TRADE_KEY"),
            api_secret=os.getenv("BINANCE_API_TRADE_SECRET"),
        )
        await self.binance.sync_exchange_info()

    async def asyncTearDown(self):
        await self.binance.close()

    async def test_exchange_info(self):
        spot = await self.binance.get_exchange_info("spot")
        self.assertTrue(spot is not None)

        futures = await self.binance.get_exchange_info("futures")
        self.assertTrue(futures is not None)

        perp = await self.binance.get_exchange_info("perp")
        self.assertTrue(perp is not None)

        exchange_info = await self.binance.get_exchange_info()
        self.assertTrue(exchange_info is not None)
        return

    async def test_get_account_info(self):
        account = await self.binance.get_spot_account_info()
        self.assertTrue(account)
        return

    async def test_get_current_funding_rate(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        funding_rate = await self.binance.get_current_funding_rate(instrument_id)
        self.assertTrue(funding_rate)

    async def test_get_funding_rate_with_num(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        num = 1333
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)

        instrument_id = "BTC/USD:BTC-PERP"
        num = 1333
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)
        return

    async def test_get_current_candlestick(self):
        instrument_id = "BTC/USDT:USDT"
        kline = await self.binance.get_current_candlestick(instrument_id, "1d")
        self.assertTrue(kline)
        return

    async def test_get_history_candlesticks(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_futures"]][0]

        num = 100
        for i in [spot, perp, futures]:
            kline = await self.binance.get_history_candlesticks(i, "1d", num=num)
            self.assertEqual(len(kline), num, msg=f"{i} {len(kline)}")
        return

    async def test_get_funding_rate_with_timestamp(self):
        # linear
        instrument_id = "BTC/USDT:USDT-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        funding_rate = await self.binance.get_history_funding_rate(instrument_id, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)

        instrument_id = "BTC/USD:BTC-PERP"
        funding_rate = await self.binance.get_history_funding_rate(instrument_id, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)
        return

    async def test_get_price(self):
        # define instrument
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_futures"]][0]
        inverse = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_inverse"]][0]

        # get last price
        for instrument_id in [spot, perp, futures, inverse]:
            last_price = await self.binance.get_last_price(instrument_id)
            self.assertTrue(last_price)

        # get index price
        for instrument_id in [spot, perp, futures, inverse]:
            index_price = await self.binance.get_index_price(instrument_id)
            self.assertTrue(index_price)

        # get mark price
        for instrument_id in [perp, futures, inverse]:
            mark_price = await self.binance.get_mark_price(instrument_id)
            self.assertTrue(mark_price)

        return

    async def test_get_open_interest(self):
        linear = "BTC/USDT:USDT-PERP"
        inverse = "BTC/USD:BTC-PERP"

        for i in [linear, inverse]:
            oi = await self.binance.get_open_interest(i)
            self.assertTrue(oi)
        return

    async def test_get_orderbook(self):
        # define instrument
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_futures"]][0]
        inverse = [k for k in self.binance.exchange_info if self.binance.exchange_info[k]["is_inverse"]][0]

        depth = 333

        # get orderbook
        for instrument_id in [spot, perp, futures, inverse]:
            orderbook = await self.binance.get_orderbook(instrument_id, depth)

            # check if orderbook depth is correct
            self.assertEqual(len(orderbook["asks"]), depth)
            self.assertEqual(len(orderbook["bids"]), depth)
        return

    async def test_margin_account_place_market_order(self):
        instrument_id = "ADA/USDT:USDT"
        side = "buy"
        volume = 922

        order = await self.binance.place_margin_market_order(instrument_id=instrument_id, side=side, volume=volume)
        self.assertTrue(order)
        return


class TestHTX(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.htx = Htx()
        await self.htx.sync_exchange_info()

    async def asyncTearDown(self):
        await self.htx.close()

    async def test_get_tickers(self):
        spot = await self.htx.get_tickers("spot")
        self.assertTrue(spot)

        futures = await self.htx.get_tickers("futures")
        self.assertTrue(futures)

        perp = await self.htx.get_tickers("perp")
        self.assertTrue(perp)

        tickers = await self.htx.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.htx.exchange_info if self.htx.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, futures]:
            ticker = await self.htx.get_ticker(i)
            self.assertTrue(ticker)
        return

    async def test_get_current_funding_rate(self):
        linear_perp = "BTC/USDT:USDT-PERP"
        inverse_perp = "BTC/USD:BTC-PERP"

        for i in [linear_perp, inverse_perp]:
            funding_rate = await self.htx.get_current_funding_rate(i)
            self.assertTrue(funding_rate)
        return

    async def test_get_history_funding_rate_with_num(self):
        linear_perp = "BTC/USDT:USDT-PERP"
        inverse_perp = "BTC/USD:BTC-PERP"

        num = 111

        for i in [linear_perp, inverse_perp]:
            funding_rate = await self.htx.get_history_funding_rate(i, num=num)
            self.assertEqual(len(funding_rate), num)
        return

    async def test_get_history_funding_rate_with_timestamp(self):
        linear_perp = "BTC/USDT:USDT-PERP"
        inverse_perp = "BTC/USD:BTC-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        for i in [linear_perp, inverse_perp]:
            funding_rate = await self.htx.get_history_funding_rate(i, start=start, end=end)
            self.assertEqual(len(funding_rate), target_num)
        return

    async def test_get_current_candlestick(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.htx.exchange_info if self.htx.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            kline = await self.htx.get_current_candlestick(i, "1d")
            self.assertTrue(kline)
        return


class TestKucoin(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.kucoin_public = Kucoin()
        await self.kucoin_public.sync_exchange_info()

    async def asyncTearDown(self):
        await self.kucoin_public.close()

    async def test_get_tickers(self):
        spot = await self.kucoin_public.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.kucoin_public.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.kucoin_public.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.kucoin_public.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            ticker = await self.kucoin_public.get_ticker(i)
            self.assertTrue(ticker)
        return

    async def test_get_last_price(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            last_price = await self.kucoin_public.get_last_price(i)
            self.assertTrue(last_price)
        return

    async def test_get_mark_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.kucoin_public.get_mark_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_index_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            index_price = await self.kucoin_public.get_index_price(i)
            self.assertTrue(index_price)
        return

    async def test_get_orderbook(self):
        perp = "BTC/USDT:USDT-PERP"
        depth = 222

        orderbook = await self.kucoin_public.get_orderbook(perp, depth)

        self.assertEqual(len(orderbook["asks"]), depth)
        self.assertEqual(len(orderbook["bids"]), depth)
        return

    async def test_get_current_funding_rate(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        funding_rate = await self.kucoin_public.get_current_funding_rate(instrument_id)
        self.assertTrue(funding_rate)
        return

    async def test_get_history_funding_rate_with_num(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        num = 333

        funding_rate = await self.kucoin_public.get_history_funding_rate(instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)
        return

    async def test_get_history_funding_rate_with_timestamp(self):
        instrument_id = "BTC/USDT:USDT-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1
        funding_rate = await self.kucoin_public.get_history_funding_rate(instrument_id, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)
        return

    async def test_get_current_candlestick(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.kucoin_public.exchange_info if self.kucoin_public.exchange_info[k]["is_futures"]][0]
        for i in [spot, perp, future]:
            kline = await self.kucoin_public.get_current_candlestick(i, "1d")
            self.assertTrue(kline)
        return


class TestBitget(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bitget = Bitget()
        await self.bitget.sync_exchange_info()

    async def asyncTearDown(self):
        await self.bitget.close()

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            ticker = await self.bitget.get_ticker(i)
            self.assertTrue(ticker)
        return

    async def test_get_tickers(self):
        spot = await self.bitget.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.bitget.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.bitget.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.bitget.get_tickers()
        self.assertTrue(tickers)
        return

    async def test_get_last_price(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            last_price = await self.bitget.get_last_price(i)
            self.assertTrue(last_price)
        return

    async def test_get_index_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.bitget.get_index_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_mark_price(self):
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, future]:
            mark_price = await self.bitget.get_mark_price(i)
            self.assertTrue(mark_price)
        return

    async def test_get_orderbook(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]
        depth = 75

        for i in [spot, perp, future]:
            orderbook = await self.bitget.get_orderbook(i, depth)
            self.assertEqual(len(orderbook["asks"]), depth)
            self.assertEqual(len(orderbook["bids"]), depth)

    async def test_get_current_funding_rate(self):
        perp = "BTC/USDT:USDT-PERP"
        futures = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [perp, futures]:
            funding_rate = await self.bitget.get_current_funding_rate(i)
            self.assertTrue(funding_rate, f"{i} {funding_rate}")
        return

    async def test_get_history_funding_rate_with_num(self):
        perp = "BTC/USDT:USDT-PERP"
        num = 120

        funding_rate = await self.bitget.get_history_funding_rate(perp, num=num)
        self.assertEqual(len(funding_rate), num, msg=f"{perp} {len(funding_rate)}")
        return

    async def test_get_history_funding_rate_with_timestamp(self):
        perp = "BTC/USDT:USDT-PERP"

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1

        funding_rate = await self.bitget.get_history_funding_rate(perp, start=start, end=end)
        self.assertEqual(len(funding_rate), target_num)
        return True

    async def test_get_current_candlestick(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bitget.exchange_info if self.bitget.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            kline = await self.bitget.get_current_candlestick(i, "1d")
            self.assertTrue(kline)
        return


class TestGateio(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gateio = Gateio()
        await self.gateio.sync_exchange_info()

    async def asyncTearDown(self):
        await self.gateio.close()

    async def test_get_tickers(self):
        spot = await self.gateio.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.gateio.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.gateio.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.gateio.get_tickers()
        self.assertTrue(tickers)

        return

    async def test_get_ticker(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.gateio.exchange_info if self.gateio.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            ticker = await self.gateio.get_ticker(i)
            self.assertTrue(ticker)

        return

    async def test_get_current_funding_rate(self):
        perp = "BTC/USDT:USDT-PERP"
        inverse = "BTC/USD:BTC-PERP"

        for i in [perp, inverse]:
            funding_rate = await self.gateio.get_current_funding_rate(i)
            self.assertTrue(funding_rate)
        return

    async def test_get_history_funding_rate_with_num(self):
        perp = "BTC/USDT:USDT-PERP"
        inverse = "BTC/USD:BTC-PERP"

        num = 111

        for i in [perp, inverse]:
            funding_rate = await self.gateio.get_history_funding_rate(i, num=num)
            self.assertEqual(len(funding_rate), num)
        return

    async def test_get_current_candlestick(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.gateio.exchange_info if self.gateio.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            kline = await self.gateio.get_current_candlestick(i, "1d")
            self.assertTrue(kline)
        return


class TestBybit(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bybit_public = Bybit()
        await self.bybit_public.sync_exchange_info()

    async def asyncTearDown(self):
        await self.bybit_public.close()

    async def test_get_tickers(self):
        spot = await self.bybit_public.get_tickers("spot")
        self.assertTrue(spot)

        perp = await self.bybit_public.get_tickers("perp")
        self.assertTrue(perp)

        future = await self.bybit_public.get_tickers("futures")
        self.assertTrue(future)

        tickers = await self.bybit_public.get_tickers()
        self.assertTrue(tickers)

        return

    async def test_get_funding_rate(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        num = 33
        funding_rate = await self.bybit_public.get_history_funding_rate(instrument_id=instrument_id, num=num)
        self.assertEqual(len(funding_rate), num)

        start = int(dt(2024, 3, 1).timestamp() * 1000)
        end = int(dt(2024, 3, 3).timestamp() * 1000)
        funding_rate = await self.bybit_public.get_history_funding_rate(
            instrument_id=instrument_id, start=start, end=end
        )
        self.assertEqual(len(funding_rate), 7)
        return

    async def test_get_current_funding_rate(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        funding_rate = await self.bybit_public.get_current_funding_rate(instrument_id)
        self.assertTrue(funding_rate)
        return

    async def test_get_open_interest(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        interval = "5m"

        oi = await self.bybit_public.get_open_interest(instrument_id, interval)
        self.assertTrue(oi)
        return

    async def test_get_orderbook(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        depth = 100
        orderbook = await self.bybit_public.get_orderbook(instrument_id, depth)
        self.assertTrue(orderbook)
        return

    async def test_get_last_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        last_price = await self.bybit_public.get_last_price(instrument_id)
        self.assertTrue(last_price)
        return

    async def test_get_index_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        index_price = await self.bybit_public.get_index_price(instrument_id)
        self.assertTrue(index_price)
        return

    async def test_get_mark_price(self):
        instrument_id = "BTC/USDT:USDT-PERP"
        mark_price = await self.bybit_public.get_mark_price(instrument_id)
        self.assertTrue(mark_price)
        return

    async def test_get_current_candlestick(self):
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"
        future = [k for k in self.bybit_public.exchange_info if self.bybit_public.exchange_info[k]["is_futures"]][0]

        for i in [spot, perp, future]:
            data = await self.bybit_public.get_current_candlestick(i, "1d")
            self.assertTrue(data)

    # Private test

    async def test_get_account_balance(self):
        balance = await self.bybit_private.get_account_balance()
        self.assertTrue(balance)
        return


class TestOutputStructure(IsolatedAsyncioTestCase):
    ticker_structure = {
        "timestamp": int,
        "instrument_id": str,
        "open_time": int,
        "close_time": int,
        "open": float,
        "high": float,
        "low": float,
        "last": float,
        "base_volume": float,
        "quote_volume": float,
        "price_change": float,
        "price_change_percent": float,
        "raw_data": dict,
    }
    current_funding_rate_structure = {
        "timestamp": int,
        "next_funding_time": int,
        "instrument_id": str,
        "market_type": str,
        "funding_rate": float,
        "raw_data": dict,
    }
    history_funding_rate_structure = {
        "timestamp": int,
        "instrument_id": str,
        "market_type": str,
        "funding_rate": float,
        "realized_rate": float,
        "raw_data": dict,
    }
    candlesticks_structure = {
        "timestamp": int,
        "instrument_id": str,
        "market_type": str,
        "interval": str,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "base_volume": float,
        "quote_volume": float,
        "contract_volume": float,
        "raw_data": list,
    }
    last_price_structure = {
        "timestamp": int,
        "instrument_id": str,
        "market_type": str,
        "last_price": float,
        "raw_data": dict,
    }
    index_price_structure = {
        "timestamp": int,
        "instrument_id": str,
        "market_type": str,
        "index_price": float,
        "raw_data": dict,
    }
    mark_price_structure = {
        "timestamp": int,
        "instrument_id": str,
        "market_type": str,
        "mark_price": float,
        "raw_data": dict,
    }

    async def asyncSetUp(self):
        self.okx_public = Okx()
        self.binace_public = Binance()
        self.kucoin_public = Kucoin()
        self.bitget_public = Bitget()
        self.bybit_public = Bybit()
        self.htx_public = Htx()
        self.gateio_public = Gateio()
        self.exchange_list = [
            self.okx_public,
            self.binace_public,
            self.kucoin_public,
            self.bitget_public,
            self.bybit_public,
            self.htx_public,
            self.gateio_public,
        ]

        for i in self.exchange_list:
            await i.sync_exchange_info()

    async def asyncTearDown(self):
        for i in self.exchange_list:
            await i.close()

    async def test_ticker_output_structure(self):
        """
        Test if the output of get_ticker() has the correct structure
        1. all the keys in the output should be the same
        2. all the values should be the correct type
        3. no other keys should be present
        """
        for exchange in self.exchange_list:
            spot = "BTC/USDT:USDT"
            perp = "BTC/USDT:USDT-PERP"
            futures = list(filter(lambda x: exchange.exchange_info[x]["is_futures"], exchange.exchange_info))[0]

            for instrument_id in [spot, perp, futures]:
                ticker = await exchange.get_ticker(instrument_id)
                print(exchange.name, ticker)
                data = ticker[instrument_id]
                # check if all the keys are the same
                self.assertEqual(
                    set(data.keys()), set(self.ticker_structure.keys()), msg=f"{exchange.name} {instrument_id} {data}"
                )

                # check if all the values are the correct type
                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value, self.ticker_structure[key], msg=f"{exchange.name} {instrument_id} {key} {value}"
                        )
        return

    async def test_current_funding_rate_structure(self):
        """
        Test if the output of get_current_funding_rate() has the correct structure.
        1. return should be a nested dictionary with instrument_id as the first key
        2. all the keys in the output should be the same
        3. all the values should be the correct type
        """

        instrument_id = "BTC/USDT:USDT-PERP"
        for exchange in self.exchange_list:
            result = await exchange.get_current_funding_rate(instrument_id)
            print(exchange.name, instrument_id, result)

            data = result[instrument_id]
            self.assertIsInstance(data, dict)

            # check if all the keys are the same
            self.assertEqual(
                set(data.keys()),
                set(self.current_funding_rate_structure.keys()),
                msg=f"{exchange.name} {instrument_id} {data}",
            )

            # check if all the values are the correct type if the value is not None
            for key, value in data.items():
                if value:
                    self.assertIsInstance(
                        value,
                        self.current_funding_rate_structure[key],
                        msg=f"{exchange.name} {instrument_id} {key} {value}",
                    )
        return

    async def test_history_funding_rate_structure(self):
        """
        Test if the output of get_history_funding_rate() has the correct structure.
        In this test will query with (start, end) and num
        1. return should be a list
        2. all the sub funding rate should follow the `history_funding_rate_structure`
        3. all the values should be the correct type
        4. (start, end) should return days * 3 + 1 funding rate
        5. num should return num funding rate

        """

        instrument_id = "BTC/USDT:USDT-PERP"
        num = 77

        delta = 10
        start = get_yesterday_timestamp() - delta * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_num = 3 * delta + 1
        acceptable_error = -1

        for exchange in self.exchange_list:
            # (num) test
            datas = await exchange.get_history_funding_rate(instrument_id, num=num)
            print(exchange.name, instrument_id, len(datas))
            self.assertEqual(len(datas), num)

            for data in datas:
                self.assertEqual(
                    set(data.keys()),
                    set(self.history_funding_rate_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value,
                            self.history_funding_rate_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )

            # (start, end) test
            datas = await exchange.get_history_funding_rate(instrument_id, start=start, end=end)
            print(exchange.name, instrument_id, len(datas))
            self.assertIn(len(datas), [target_num, target_num + acceptable_error])

            for data in datas:
                self.assertEqual(
                    set(data.keys()),
                    set(self.history_funding_rate_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value,
                            self.history_funding_rate_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )
        return

    async def test_get_current_candlestick_structure(self):
        """
        Test if the output of get_current_candlestick() has the correct structure.
        1. return should be a dict
        2. all the keys in the output should be the same
        3. all the values should be the correct type
        4. test volume correctness:
            - difference between quote_volume / base_volume and last price should be less than 5%
            - contract_volume = base_volume * contract_size or quote_volume / contract_size
        """

        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"

        for exchange in self.exchange_list:
            futures = [k for k, v in exchange.exchange_info.items() if v["is_futures"]][0]
            for instrument_id in [spot, perp, futures]:
                current_candlestick = await exchange.get_current_candlestick(instrument_id, "1d")
                ticker = await exchange.get_ticker(instrument_id)
                print(exchange.name, instrument_id, current_candlestick)

                data = current_candlestick[instrument_id]
                ticker_data = ticker[instrument_id]
                info = exchange.exchange_info[instrument_id]
                self.assertIsInstance(data, dict)

                # check if all the keys are the same
                self.assertEqual(
                    set(data.keys()),
                    set(self.candlesticks_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                # check if all the values are the correct type
                for key, value in data.items():
                    if value and key != "raw_data":
                        self.assertIsInstance(
                            value,
                            self.candlesticks_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )

                if not data["base_volume"] or not data["quote_volume"]:
                    continue
                # check if the volume is correct
                implied_price = data["quote_volume"] / data["base_volume"]
                error_window = 0.05
                last = ticker_data["last"]

                self.assertLess(
                    abs(implied_price - last) / last,
                    error_window,
                    msg=f"{exchange.name} {instrument_id}\n"
                    f"implied_price: {implied_price}\n"
                    f"last: {last}\n"
                    f"data: {data}\n"
                    f"info: {info}",
                )

                if not info["contract_size"]:
                    continue
                # check if the contract volume is correct
                contract_size = info["contract_size"]
                contract_volume = data["contract_volume"]
                implied_volumes = [data["base_volume"] / contract_size, data["quote_volume"] / contract_size]

                error_list = [abs(contract_volume - v) / v for v in implied_volumes]
                error_window = 0.05
                self.assertTrue(
                    any([error < error_window for error in error_list]),
                    msg=f"{exchange.name} {instrument_id}\n"
                    f"contract_size: {contract_size}\n"
                    f"contract_volume: {contract_volume}\n"
                    f"implied_volume: {implied_volumes}\n"
                    f"data: {data}\n"
                    f"info: {info}",
                )
        return

    async def test_get_history_candlesticks(self):
        """
        Test if the output of get_history_candlesticks() has the correct structure.
        1. return should be dict of a list, instrument_id as the first key
        2. all the keys in the output should be the same
        3. all the values should be the correct type
        4. test different query method
            - inputs (num) -> num of the output should be equal to num
            - inputs (start, end) -> num of the output should be equal to (end - start) / interval
        """

        delta_days = 10
        start = get_yesterday_timestamp() - delta_days * 24 * 60 * 60 * 1000
        end = get_yesterday_timestamp()
        target_nums = [delta_days + 1, delta_days]

        num = 107

        interval = "1d"

        for exchange in self.exchange_list:

            spot = "BTC/USDT:USDT"
            perp = "BTC/USDT:USDT-PERP"
            # futures = [k for k, v in exchange.exchange_info.items() if v["is_futures"]][0]

            for instrument_id in [spot, perp]:
                params = {"instrument_id": instrument_id, "interval": interval}
                # (num) test
                datas = await exchange.get_history_candlesticks(**params, num=num)
                print(exchange.name, instrument_id, len(datas))

                self.assertEqual(
                    len(datas), 90 if exchange.name == "bitget" and num > 90 and instrument_id == perp else num
                )

                for data in datas:
                    self.assertEqual(
                        set(data.keys()),
                        set(self.candlesticks_structure.keys()),
                        msg=f"{exchange.name} {instrument_id} {data}",
                    )

                    for key, value in data.items():
                        if value and key != "raw_data":
                            self.assertIsInstance(
                                value,
                                self.candlesticks_structure[key],
                                msg=f"{exchange.name} {instrument_id} {key} {value}",
                            )
                if exchange.name == "htx" and instrument_id == spot:
                    continue
                # (start, end) test
                datas = await exchange.get_history_candlesticks(**params, start=start, end=end)
                print(exchange.name, instrument_id, len(datas))

                self.assertTrue(len(datas) in target_nums, msg=f"{exchange.name} {instrument_id} {len(datas)}")

                for data in datas:
                    self.assertEqual(
                        set(data.keys()),
                        set(self.candlesticks_structure.keys()),
                        msg=f"{exchange.name} {instrument_id} {data}",
                    )

                    for key, value in data.items():
                        if value and key != "raw_data":
                            self.assertIsInstance(
                                value,
                                self.candlesticks_structure[key],
                                msg=f"{exchange.name} {instrument_id} {key} {value}",
                            )
        return

    async def test_price_related_structure(self):
        """
        Test if the output of get_last_price(), get_index_price(), get_mark_price() has the correct structure.
        1. return should be dict, instrument_id as the first key
        2. all the keys in the output should be the same
        3. all the values should be the correct type

        """
        spot = "BTC/USDT:USDT"
        perp = "BTC/USDT:USDT-PERP"

        for exchange in self.exchange_list:

            futures = [k for k, v in exchange.exchange_info.items() if v["is_futures"]][0]

            for instrument_id in [spot, perp, futures]:
                # last price
                data = await exchange.get_last_price(instrument_id)
                print(f"Last price: {exchange.name} {instrument_id} {data}")

                self.assertIsInstance(data, dict)
                self.assertEqual(
                    set(data.keys()),
                    set(self.last_price_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value,
                            self.last_price_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )

                if instrument_id == spot:
                    continue

                # index price
                data = await exchange.get_index_price(instrument_id)
                print(f"Index price: {exchange.name} {instrument_id} {data}")

                self.assertIsInstance(data, dict)
                self.assertEqual(
                    set(data.keys()),
                    set(self.index_price_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value,
                            self.index_price_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )

                # mark price
                data = await exchange.get_mark_price(instrument_id)
                print(f"Mark price: {exchange.name} {instrument_id} {data}")

                self.assertIsInstance(data, dict)
                self.assertEqual(
                    set(data.keys()),
                    set(self.mark_price_structure.keys()),
                    msg=f"{exchange.name} {instrument_id} {data}",
                )

                for key, value in data.items():
                    if value:
                        self.assertIsInstance(
                            value,
                            self.mark_price_structure[key],
                            msg=f"{exchange.name} {instrument_id} {key} {value}",
                        )

        return


class TestWoo(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.woo = WOO()
        await self.woo.sync_exchange_info()

    async def asyncTearDown(self):
        await self.woo.close()

    async def test_get_historical_candlesticks(self):
        interval = "1d"
        instrument_id = "1000LUNC/USDT:USDT-PERP"
        num = 1000

        datas = await self.woo.get_history_candlesticks(instrument_id=instrument_id, interval=interval, num=num)
        df = pd.DataFrame(datas["rows"])
        df["start"] = df["start_timestamp"].apply(lambda x: dt.fromtimestamp(x / 1000))
        df["end"] = df["end_timestamp"].apply(lambda x: dt.fromtimestamp(x / 1000))
        df.to_csv("woo_api_lunc.csv", index=False)
        return True
