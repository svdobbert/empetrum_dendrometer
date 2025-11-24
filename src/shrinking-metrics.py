import pandas as pd
import numpy as np
import calendar


df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["week"] = df["date"].dt.isocalendar().week
df["month"] = df["date"].dt.month
df["doy"] = df["date"].dt.dayofyear
df["group"] = df["id"] + " " + df["year"].astype(str) + " " + df["week"].astype(str)
df = df.dropna(subset=["D_mean"])
print(df.head())

df["change"] = df.groupby("id")["D_mean"].diff()
weekly_total_change = df.groupby("group")["change"].sum().reset_index()
weekly_total_change["date"] = pd.to_datetime(weekly_total_change["group"].str.split(" ").str[1] + "-" + weekly_total_change["group"].str.split(" ").str[2] + "-1", format="%Y-%W-%w")
weekly_total_change["year"] = weekly_total_change["date"].dt.year
weekly_total_change["week"] = weekly_total_change["group"].str.split(" ").str[2].astype(int)
weekly_total_change["id"] = weekly_total_change["group"].str.split(" ").str[0]

shrinking_value = -2
change_year = df.groupby(["id", "year"])["change"].sum().to_numpy()
print(len(change_year))
shrinking_year = np.minimum(change_year, 0)
print(len(shrinking_year))
growth_year = np.maximum(change_year, 0)
print(len(growth_year))
first_neg_per_group = df.groupby(["id", "year"]).apply(lambda g: g.loc[g["change"] < shrinking_value, "date"].min())
first_shrinking_year = df.set_index(["id", "year"]).index.map(first_neg_per_group)
first_shrinking_year = np.array(first_shrinking_year, dtype="datetime64[ns]")
print(len(first_shrinking_year))
print(first_shrinking_year)
n_shrinking_days = df.groupby(["id", "year"])["change"].apply(lambda s: s.lt(0).sum()).to_numpy()
print(len(n_shrinking_days))

def calculate_before_shrinking(n, env, function="sum", first_neg=first_neg_per_group):
    sum_before_shrinking = []

    for (id_val, _year), first_shrink_date in first_neg.items():
        start_date = first_shrink_date - pd.Timedelta(days=n)
        end_date = first_shrink_date

        if function == "sum":
            sum_value = df[(df["id"] == id_val) & (df["date"] >= start_date) & (df["date"] < end_date)][env].sum()
        elif function == "mean":
            sum_value = df[(df["id"] == id_val) & (df["date"] >= start_date) & (df["date"] < end_date)][env].mean()
        elif function == "max":
            sum_value = df[(df["id"] == id_val) & (df["date"] >= start_date) & (df["date"] < end_date)][env].max()
        elif function == "min":
            sum_value = df[(df["id"] == id_val) & (df["date"] >= start_date) & (df["date"] < end_date)][env].min()
        elif function == "time_min":
            sub = df[(df["id"] == id_val) & (df["date"] >= start_date) & (df["date"] < end_date)]
            if sub.empty or sub[env].isna().all():
                sum_value = np.nan
            else:
                min_idx = sub[env].idxmin()
                min_date = df.loc[min_idx, "date"]
                sum_value = (end_date - min_date).days
        else:
            raise ValueError("Invalid function. Choose from 'sum', 'mean', 'max', 'min', 'time_min'.")

        sum_before_shrinking.append(sum_value)
        
    print(len(sum_before_shrinking))
    print(n, env, function)
    return np.array(sum_before_shrinking)

shrinking_df = pd.DataFrame({
    "shrinking_year": shrinking_year,
    "growth_year": growth_year,
    "n_shrinking_days": n_shrinking_days,
    "first_shrinking_date": first_neg_per_group.values,  
    "first_shrinking_doy": first_neg_per_group.dt.dayofyear.values,
    "sum_AT_7": calculate_before_shrinking(7, "AT_mean", "sum"),
    "mean_AT_7": calculate_before_shrinking(7, "AT_mean", "mean"),
    "max_AT_7": calculate_before_shrinking(7, "AT_mean", "max"),
    "min_AT_7": calculate_before_shrinking(7, "AT_mean", "min"),
    "time_min_AT_7": calculate_before_shrinking(7, "AT_mean", "time_min"),
    "sum_ST_7": calculate_before_shrinking(7, "ST_mean", "sum"),
    "mean_ST_7": calculate_before_shrinking(7, "ST_mean", "mean"),
    "max_ST_7": calculate_before_shrinking(7, "ST_mean", "max"),
    "min_ST_7": calculate_before_shrinking(7, "ST_mean", "min"),
    "time_min_ST_7": calculate_before_shrinking(7, "ST_mean", "time_min"),
    "sum_SM_7": calculate_before_shrinking(7, "SM_mean", "sum"),
    "mean_SM_7": calculate_before_shrinking(7, "SM_mean", "mean"),
    "max_SM_7": calculate_before_shrinking(7, "SM_mean", "max"),
    "min_SM_7": calculate_before_shrinking(7, "SM_mean", "min"),
    "time_min_SM_7": calculate_before_shrinking(7, "SM_mean", "time_min"),
    "sum_AT_30": calculate_before_shrinking(30, "AT_mean", "sum"),
    "mean_AT_30": calculate_before_shrinking(30, "AT_mean", "mean"),
    "max_AT_30": calculate_before_shrinking(30, "AT_mean", "max"),
    "min_AT_30": calculate_before_shrinking(30, "AT_mean", "min"),
    "time_min_AT_30": calculate_before_shrinking(30, "AT_mean", "time_min"),
    "sum_ST_30": calculate_before_shrinking(30, "ST_mean", "sum"),
    "mean_ST_30": calculate_before_shrinking(30, "ST_mean", "mean"),
    "max_ST_30": calculate_before_shrinking(30, "ST_mean", "max"),
    "min_ST_30": calculate_before_shrinking(30, "ST_mean", "min"),
    "time_min_ST_30": calculate_before_shrinking(30, "ST_mean", "time_min"),
    "sum_SM_30": calculate_before_shrinking(30, "SM_mean", "sum"),
    "mean_SM_30": calculate_before_shrinking(30, "SM_mean", "mean"),
    "max_SM_30": calculate_before_shrinking(30, "SM_mean", "max"),
    "min_SM_30": calculate_before_shrinking(30, "SM_mean", "min"),
    "time_min_SM_30": calculate_before_shrinking(30, "SM_mean", "time_min"),
    "sum_AT_90": calculate_before_shrinking(90, "AT_mean", "sum"),
    "mean_AT_90": calculate_before_shrinking(90, "AT_mean", "mean"),
    "max_AT_90": calculate_before_shrinking(90, "AT_mean", "max"),
    "min_AT_90": calculate_before_shrinking(90, "AT_mean", "min"),
    "time_min_AT_90": calculate_before_shrinking(90, "AT_mean", "time_min"),
    "sum_ST_90": calculate_before_shrinking(90, "ST_mean", "sum"),
    "mean_ST_90": calculate_before_shrinking(90, "ST_mean", "mean"),
    "max_ST_90": calculate_before_shrinking(90, "ST_mean", "max"),
    "min_ST_90": calculate_before_shrinking(90, "ST_mean", "min"),
    "time_min_ST_90": calculate_before_shrinking(90, "ST_mean", "time_min"),
    "sum_SM_90": calculate_before_shrinking(90, "SM_mean", "sum"),
    "mean_SM_90": calculate_before_shrinking(90, "SM_mean", "mean"),
    "max_SM_90": calculate_before_shrinking(90, "SM_mean", "max"),
    "min_SM_90": calculate_before_shrinking(90, "SM_mean", "min"),
    "time_min_SM_90": calculate_before_shrinking(90, "SM_mean", "time_min"),
    "sum_AT_365": calculate_before_shrinking(365, "AT_mean", "sum"),
    "mean_AT_365": calculate_before_shrinking(365, "AT_mean", "mean"),
    "sum_ST_365": calculate_before_shrinking(365, "ST_mean", "sum"),
    "mean_ST_365": calculate_before_shrinking(365, "ST_mean", "mean"),
    "sum_SM_365": calculate_before_shrinking(365, "SM_mean", "sum"),
    "mean_SM_365": calculate_before_shrinking(365, "SM_mean", "mean"),
    "sum_AT_1095": calculate_before_shrinking(1095, "AT_mean", "sum"),
    "mean_AT_1095": calculate_before_shrinking(1095, "AT_mean", "mean"),
    "sum_ST_1095": calculate_before_shrinking(1095, "ST_mean", "sum"),
    "mean_ST_1095": calculate_before_shrinking(1095, "ST_mean", "mean"),
    "sum_SM_1095": calculate_before_shrinking(1095, "SM_mean", "sum"),
    "mean_SM_1095": calculate_before_shrinking(1095, "SM_mean", "mean"),
}, index=first_neg_per_group.index)

shrinking_df = shrinking_df.reset_index()

cols = ["AT_mean", "ST_mean", "SM_mean"]
monthly_means = (
    df.groupby(["id", "year", "month"])[cols]
      .mean()              
      .reset_index()
)

for var in cols:
    pivot = monthly_means.pivot(index=["id", "year"], columns="month", values=var)
    pivot.columns = [f"{var}_{calendar.month_abbr[int(m)]}" for m in pivot.columns]
    pivot = pivot.reset_index()
    shrinking_df = shrinking_df.merge(pivot, on=["id", "year"], how="left")

print(shrinking_df.head())
shrinking_df.to_csv("input/shrinking-metrics.csv", index=False)
