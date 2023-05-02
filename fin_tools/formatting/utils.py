import polars as pl


def df_to_dict(df: pl.DataFrame) -> dict:
    """convert dataframe to dict format required by plotly"""
    return {k: v.to_list() for k, v in df.sort("tick_dir_imbal_bar_id").to_dict().items()}
