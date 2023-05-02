import asyncio

from fin_tools.aggregations import BarMaker
from fin_tools.clients import Binance
from fin_tools.formatting import df_to_dict


async def main():
    b = Binance()

    df = await b.pull_data()
    df = BarMaker.create_imbalance_bars(df, "tick_dir", 10).rename({"tick_dir_imbal_bar_id": "x"})
    # print(df.columns)
    print(df_to_dict(df, sort="x"))


if __name__ == "__main__":
    asyncio.run(main())
