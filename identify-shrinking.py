import pandas as pd
from scipy.stats import linregress

exec(open('constants/palettes.py').read())

df = pd.read_csv("input/daily_data.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month

def get_trend(group):
    if group["D_mean"].iloc[-1] < group["D_mean"].iloc[0]:
        return "shrinking"
    else:
        return "growth"
    
def trend_slope(group):
    slope = linregress(group.index, group["D_mean"]).slope
    return "shrinking" if slope < 0 else "growth"

trend_series = df.groupby(["id", "year"]).apply(trend_slope)
trend_series.name = "trend"

df = df.merge(trend_series, on=["id", "year"])
print(df.head())
df.to_csv("input/daily_data_with_trends.csv", index=False)
