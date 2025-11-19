import pandas as pd
from scipy.stats import linregress

exec(open('constants/palettes.py').read())

df = pd.read_csv("input/daily_data.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month

def get_shrinking(group):
    valid = group["D_mean"].dropna()
    if valid.empty:
        change = float('nan')
    else:
        change = valid.iloc[-1] - valid.iloc[0]
    return change
    
trend_df = df.groupby(["id", "year"]).apply(get_shrinking).reset_index(name="change")

print(trend_df)