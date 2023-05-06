import asyncio
import time

import polars as pl

from fin_tools.aggregations import BarMaker
from fin_tools.clients import Binance
from fin_tools.formatting import df_to_dict


async def main():
    for i in [0, 1, 2]:
        tx_history = pl.DataFrame()

        b = Binance()

        df = await b.pull_data()
        await b.close()

        df.write_csv(f"fin_tools/aggregations/tests/assets/sample_tx_{i}.csv")
        time.sleep(4)

        # df = BarMaker.create_imbalance_bars(df, "tick_dir", 10)
        # # print(df.columns)
        # print(df_to_dict(df, sort="tick_dir_imbal_bar_id"))


if __name__ == "__main__":
    asyncio.run(main())
