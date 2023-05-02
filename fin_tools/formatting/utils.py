import polars as pl


def df_to_dict(df: pl.DataFrame, sort: str) -> dict:
    """convert dataframe to dict format required by plotly"""
    return {k: v.to_list() for k, v in df.sort(sort).to_dict().items()}
