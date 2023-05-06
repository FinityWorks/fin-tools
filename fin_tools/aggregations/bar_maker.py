from typing import Tuple

import polars as pl


class BarMaker:
    def __init__(self, quantifier="tick_dir", imbal_limit=10, bar_length_limit=200) -> None:
        self.quantifier = quantifier
        self.imbal_limit = imbal_limit
        self.bar_length_limit = bar_length_limit

        self.bars = pl.DataFrame()
        self.remaining_ticks = pl.DataFrame()

    @staticmethod
    def cum_boundary(vals, bound_max):
        """gets a cumulative imbalance aggregation grouping"""
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

    def create_imbalance_bars(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        events, resets = self.cum_boundary(df[self.quantifier].to_list(), self.imbal_limit)

        df = (
            df.with_columns(
                [
                    pl.Series(name=f"{self.quantifier}_imbal", values=events),
                    pl.Series(name="reset", values=resets),
                ]
            )
            .with_columns(pl.col("reset").cumsum().alias(f"{self.quantifier}_imbal_bar_id"))
            .drop([f"{self.quantifier}_imbal", "reset"])
        )

        df_tail = df.filter(
            pl.col(f"{self.quantifier}_imbal_bar_id")
            == pl.col(f"{self.quantifier}_imbal_bar_id").max()
        ).drop(f"{self.quantifier}_imbal_bar_id")

        df = (
            (
                df.groupby(f"{self.quantifier}_imbal_bar_id")
                .agg(
                    [
                        pl.min("id").alias(f"bar_id"),
                        pl.first("price").alias("open"),
                        pl.max("price").alias("high"),
                        pl.min("price").alias("low"),
                        pl.last("price").alias("close"),
                    ]
                )
                .sort(f"{self.quantifier}_imbal_bar_id")
            )
            .drop(f"{self.quantifier}_imbal_bar_id")
            .rename({"bar_id": f"{self.quantifier}_imbal_bar_id"})
        )

        return df, df_tail

    def update_bars(self, new_ticks):
        ticks = pl.concat([self.remaining_ticks, new_ticks])

        if len(self.remaining_ticks) > 0:
            min_val = self.remaining_ticks["id"].min()
            ticks = ticks.filter(pl.col("id") > min_val)

        ticks = ticks.groupby("id").first()
        new_bar_df, self.remaining_ticks = self.create_imbalance_bars(ticks)

        if len(self.bars) > 0:
            self.bars = pl.concat([self.bars.slice(0, -1), new_bar_df])
        else:
            self.bars = new_bar_df

        if len(self.bars) > self.bar_length_limit:
            self.bars = self.bars.slice(len(self.bars) - self.bar_length_limit - 1, -1)

        self.bars = self.bars.sort(f"{self.quantifier}_imbal_bar_id")


if __name__ == "__main__":
    import pathlib

    maker = BarMaker(imbal_limit=10, quantifier="tick_dir", bar_length_limit=40)

    resp_path = pathlib.Path(__file__).parent.resolve() / "tests" / "assets"
    df0 = pl.read_csv(resp_path / "sample_tx_0.csv")
    df1 = pl.read_csv(resp_path / "sample_tx_1.csv")
    df2 = pl.read_csv(resp_path / "sample_tx_2.csv")

    maker.update_bars(df0)
    print(maker.bars)

    maker.update_bars(df1)
    print(maker.bars)

    maker.update_bars(df2)
    print(maker.bars)
