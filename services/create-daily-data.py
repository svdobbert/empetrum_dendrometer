import duckdb
import pandas as pd

def create_daily_data_duckdb(file_path, date_col="datetime"):
    """
    Create a daily averaged dataframe from a CSV/Parquet using DuckDB.
    
    Parameters
    ----------
    file_path : str
        Path to the CSV or Parquet file with datetime, id, and numeric values.
    date_col : str
        Name of the datetime column (default: 'datetime').
    value_col : str
        Name of the numeric value column to average (default: 'D').
    
    Returns
    -------
    pd.DataFrame
        A daily averaged dataframe grouped by date and id.
    """
    con = duckdb.connect()

    query = f"""
    SELECT CAST({date_col} AS DATE) AS date,
           id,
           AVG("D") AS D_mean,
           AVG("AT") AS AT_mean,
           AVG("ST") AS ST_mean,
           AVG("SM") AS SM_mean
    FROM read_csv_auto('{file_path}', SAMPLE_SIZE=-1)
    GROUP BY date, id
    ORDER BY date, id
    """

    return con.execute(query).df()

df_daily = create_daily_data_duckdb("input/input.csv")

split_cols = df_daily["id"].str.split(".", expand=True)
split_cols.columns = ["country", "site", "individuum", "branch", "section", "radius"]
df_daily = pd.concat([df_daily[["date", "D_mean", "AT_mean", "ST_mean", "SM_mean", "id"]], split_cols], axis=1)

df_daily.to_csv("input/daily_data.csv", index=False)
