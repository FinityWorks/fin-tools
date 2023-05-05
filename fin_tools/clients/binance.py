import ccxt.async_support as ccxt
import numpy as np
import polars as pl


class Binance(ccxt.binance):
    def __init__(self):
        super().__init__()
        self.set_sandbox_mode(True)

    async def pull_data(self, ticker="BTC/USDT") -> pl.DataFrame:
        """pull trades, reformat, sort dates, create delta_p and tick dir"""
        abbrs = {
            "M": "info_market",  # true if market, false if limit
            "T": "info_timestamp",
            "a": "info_amount",
            "f": "info_fee",
            "l": "info_liquidity",
            "m": "info_maker",  # true if maker, false if taker
            "p": "info_price",
            "q": "info_quantity",
        }

        tdf = await self.fetch_trades(ticker)

        tdf = (
            pl.DataFrame([{**x.pop("info"), **x} for x in tdf])
            .rename(abbrs)
            .select(["id", "symbol", "datetime", "side", "price", "amount", "cost"])
        )

        tdf = (
            tdf.sort("datetime")
            .with_columns(
                [
                    pl.col("price").diff().alias("delta_p"),
                ]
            )
            .drop_nulls()
            .with_columns(
                [
                    pl.col("delta_p").apply(np.sign).alias("tick_dir"),
                ]
            )
        )
        return tdf
