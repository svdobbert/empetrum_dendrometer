import duckdb
import pandas as pd


def create_monthly_data_duckdb(file_path, date_col="datetime"):
    """
    Create a monthly averaged dataframe from a CSV/Parquet using DuckDB,
    and pivot so that months appear as columns (e.g. 'Jan_AT').
    Adds a 'year' column to specify the year for each row.
    
    Parameters
    ----------
    file_path : str
        Path to the CSV or Parquet file with datetime, id, and numeric values.
    date_col : str
        Name of the datetime column (default: 'datetime').
    
    Returns
    -------
    pd.DataFrame
        A monthly averaged dataframe (wide format) with months as columns and year column.
    """
    con = duckdb.connect()
    
    query = f"""
    SELECT 
        YEAR({date_col}) AS year,
        MONTH({date_col}) AS month_num,
        id,
        AVG("AT") AS AT_mean,
        AVG("ST") AS ST_mean,
        AVG("SM") AS SM_mean
    FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1)
    GROUP BY year, month_num, id
    ORDER BY id, year, month_num
    """
    
    df_monthly = con.execute(query).df()
    
    df_monthly["month_name"] = pd.to_datetime(df_monthly["month_num"], format='%m').dt.strftime('%b')
    
    df_pivot = df_monthly.pivot(
        index=["id", "year"],
        columns="month_name",
        values=["AT_mean", "ST_mean", "SM_mean"]
    )
    
    new_cols = []
    for var, month in df_pivot.columns:
        if pd.isna(month):
            new_cols.append(f"{var}_NA")
        else:
            var_short = var.replace("_mean", "")
            new_cols.append(f"{month}_{var_short}")
    
    df_pivot.columns = new_cols
    df_pivot = df_pivot.reset_index()
    
    return df_pivot

df_monthly = create_monthly_data_duckdb("input/input.csv")

split_cols = df_monthly["id"].str.split(".", expand=True)
split_cols.columns = ["country", "site", "individuum", "branch", "section", "radius"]
df_monthly = pd.concat([df_monthly, split_cols], axis=1)

df_monthly.to_csv("input/monthly_data.csv", index=False)

