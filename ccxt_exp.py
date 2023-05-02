import polars as pl


def cum_boundary(vals, bound_max):
    new_vals = [vals[0]]
    resets = [False]
    cusum = vals[0]
    for i in range(1, len(vals)):
        test_val = cusum + vals[i]

        if test_val > bound_max:
            cusum = test_val % bound_max
            resets.append(1)
        elif test_val < -bound_max:
            cusum = (test_val % bound_max) - bound_max
            resets.append(1)
        else:
            cusum = test_val
            resets.append(0)
        new_vals.append(cusum)

    return new_vals, resets


def create_imbalance_bars(df: pl.DataFrame, col: str, imbal_limit: float):
    events, resets = cum_boundary(df[col].to_list(), imbal_limit)

    df = (
        df.with_columns(
            [
                pl.Series(name=f"{col}_imbal", values=events),
                pl.Series(name="reset", values=resets),
            ]
        )
        .with_columns(pl.col("reset").cumsum().alias(f"{col}_imbal_bar_id"))
        .drop([f"{col}_imbal", "reset"])
    )

    df = df.groupby("tick_dir_imbal_bar_id").agg(
        [
            pl.first("price").alias("open"),
            pl.max("price").alias("high"),
            pl.min("price").alias("low"),
            pl.last("price").alias("close"),
        ]
    )

    return df


def pull_data():
    # pprint(ccxt.exchanges)
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

    # binance = ccxt.binance()

    #! prevent silliness occurring
    # binance.set_sandbox_mode(True)

    # trades = binance.fetch_trades("BTC/USDT")
    # pl.DataFrame([{**x.pop("info"), **x} for x in trades]).rename(abbrs).select(
    #     ["datetime", "side", "symbol", "price", "amount", "cost"]
    # ).write_csv("ticks.csv")
    # exit()


def main():
    trades = pl.read_csv("ticks.csv")

    trades = (
        trades.sort("datetime")
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
                pl.col("delta_p").apply(lambda x: (abs(x) / x) if x != 0 else 0).alias("tick_dir"),
            ]
        )
    )

    df_tick_imbal = create_imbalance_bars(trades, "tick_dir", imbal_limit=5)

    print(
        {k: v.to_list() for k, v in df_tick_imbal.sort("tick_dir_imbal_bar_id").to_dict().items()}
    )


if __name__ == "__main__":
    from fin_tools.aggregations import BarMaker
    from fin_tools.clients import Binance
    from fin_tools.formatting import df_to_dict

    df = Binance().pull_data()
    df = BarMaker.create_imbalance_bars(df, "tick_dir", 10)
    print(df_to_dict(df))
