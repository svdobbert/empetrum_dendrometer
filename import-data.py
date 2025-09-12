from functools import reduce
import pandas as pd
import duckdb


filepath = 'input/Shrinking_Emp.h__D_AT_ST_SM_Coord__2009_2024__JL054__f_Svenja05.xlsx'

dfD = pd.read_excel(filepath, sheet_name='D')

dfAT = pd.read_excel(filepath, sheet_name='AT')

dfST = pd.read_excel(filepath, sheet_name='ST')

dfSM = pd.read_excel(filepath, sheet_name='SM')

dfD_long = dfD.melt(
    id_vars=["datetime"],
    var_name="id",
    value_name="D"
)

split_cols = dfD_long["id"].str.split(".", expand=True)

split_cols.columns = ["country", "site", "individuum", "branch", "section", "radius"]

dfD_long = pd.concat([dfD_long[["datetime", "D", "id"]], split_cols[["site"]]], axis=1)

print("Dendrometer data transposed to long format.")

env = [dfAT, dfST, dfSM]

env_long = [
    df.melt(id_vars=["datetime"], var_name="site", value_name=f"env{i+1}")
    for i, df in enumerate(env)
]

print("Environmental data transposed to long format.")

env_df = reduce(
    lambda left, right: left.merge(right, on=["datetime", "site"], how="left"),
    env_long
)

print("Environmental data merged")

con = duckdb.connect()
con.register("dfD_long", dfD_long)
con.register("env_df", env_df)

df_final = con.execute("""
    SELECT d.*, e.*
    FROM dfD_long d
    LEFT JOIN env_df e
    ON d.datetime = e.datetime AND d.site = e.site
""").df()

df_final = df_final.rename(columns={
    "env1": "AT",
    "env2": "ST",
    "env3": "SM"
})

df_final = df_final[["datetime",  "site", "id", "D", "AT", "ST", "SM"]]

print(df_final)
df_final.to_csv("input/input.csv", index=False)
