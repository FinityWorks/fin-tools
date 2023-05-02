import ccxt
import numpy as np
import polars as pl


class Binance(ccxt.binance):
    def __init__(self):
        super().__init__()
        self.set_sandbox_mode(True)

    def pull_data(self, ticker="BTC/USDT") -> pl.DataFrame:
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

        tdf = self.fetch_trades(ticker)

        tdf = (
            pl.DataFrame([{**x.pop("info"), **x} for x in tdf])
            .rename(abbrs)
            .select(["symbol", "datetime", "side", "price", "amount", "cost"])
        )

        tdf = (
            tdf.sort("datetime")
            .with_columns(
                [
                    pl.lit(1).alias("tick"),
                    pl.col("price").diff().alias("delta_p"),
                ]
            )
            .drop_nulls()
            .with_columns(
                [
                    pl.col("tick").cumsum().alias("tick"),
                    pl.col("delta_p").apply(np.sign).alias("tick_dir"),
                ]
            )
        )
        return tdf
