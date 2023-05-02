import polars as pl


class BarMaker:
    def __init__(self) -> None:
        pass

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

    @classmethod
    def create_imbalance_bars(cls, df: pl.DataFrame, col: str, imbal_limit: float) -> pl.DataFrame:
        events, resets = cls.cum_boundary(df[col].to_list(), imbal_limit)

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

        df = (
            df.groupby(f"{col}_imbal_bar_id")
            .agg(
                [
                    pl.first("price").alias("open"),
                    pl.max("price").alias("high"),
                    pl.min("price").alias("low"),
                    pl.last("price").alias("close"),
                ]
            )
            .sort(f"{col}_imbal_bar_id")
        )

        return df
