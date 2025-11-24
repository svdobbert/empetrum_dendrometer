import pandas as pd
from scipy.stats import linregress

exec(open('src/constants/palettes.py').read())

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

def remove_initial(series):
    if series.notna().any():
        print('NotNA')
        return series - series.dropna().iloc[0]
    else:
        return pd.Series([float('nan')] * len(series), index=series.index)

trend_series = df.groupby(["id", "year"]).apply(get_trend)
trend_series.name = "trend"

df = df.merge(trend_series, on=["id", "year"])

df["normalized"] = df.groupby("id")["D_mean"].transform(remove_initial)

print(df.head())
print(df["normalized"].describe())

for name, group in df.groupby("id"):
    print(name)
    print(group["normalized"])
    break

df.to_csv("input/daily_data_with_trends.csv", index=False)
