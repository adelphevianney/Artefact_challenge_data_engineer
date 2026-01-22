import pandas as pd


def filter_sales_data(df: pd.DataFrame, target_date_str: str) -> pd.DataFrame:
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    target_date = pd.to_datetime(target_date_str, format="%Y%m%d").date()

    df = df[df["sale_date"] == target_date].copy()
    if df.empty:
        return df

    df["date_id"] = df["sale_date"].apply(
        lambda d: int(pd.Timestamp(d).strftime("%Y%m%d"))
    )

    df["discount_percent"] = (
        df["discount_percent"]
        .astype(str)
        .str.replace("%", "")
        .astype(float)
        .fillna(0.0)
    )

    df["discounted"] = df["discounted"].astype(bool)
    return df.where(pd.notnull, None)
