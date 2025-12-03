import pandas as pd
import numpy as np
import calendar


df_events = pd.read_csv("input/shrinking-events.csv")
df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["week"] = df["date"].dt.isocalendar().week
df["month"] = df["date"].dt.month
df["doy"] = df["date"].dt.dayofyear
df = df.dropna(subset=["D_mean"])

df["change"] = df.groupby("id")["D_mean"].diff()

def calculate_before_shrinking(df, ids, dates, n, env, function="sum"):
    results = []

    for id_val, date in zip(ids, dates):
        start_date = date - pd.Timedelta(days=n)
        end_date = date

        sub = df[(df["id"] == id_val) &
                 (df["date"] >= start_date) &
                 (df["date"] < end_date)]

        if function == "sum":
            val = sub[env].sum()
        elif function == "mean":
            val = sub[env].mean()
        elif function == "max":
            val = sub[env].max()
        elif function == "min":
            val = sub[env].min()
        elif function == "time_min":
            if sub.empty or sub[env].isna().all():
                val = np.nan
            else:
                min_idx = sub[env].idxmin()
                min_date = df.loc[min_idx, "date"]
                val = (end_date - min_date).days
        else:
            raise ValueError("Invalid function")

        results.append(val)
    print(env, n)

    return np.array(results)

event_ids = df_events["id"].values
start_shrink = pd.to_datetime(df_events["start"]).values
stop_shrink = pd.to_datetime(df_events["stop"]).values
recovery = pd.to_datetime(df_events["recovery"]).values

shrinking_df = pd.DataFrame({
    "year": df_events["year"],
    "start": df_events["start"],
    "stop": df_events["stop"],
    "recovery": df_events["recovery"],
    "shrink_days": df_events["shrink_days"],
    "total_days": df_events["total_days"],
    "start_doy": df_events["start_doy"],
    "end_doy": df_events["end_doy"],
    "recovery_doy": df_events["recovery_doy"],
    "total_shrink": df_events["total_shrink"],
    "shrink_pct": df_events["shrink_pct"],
    "depth": df_events["depth"],
    "sum_AT_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "AT_mean", "sum"),
    "mean_AT_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "AT_mean", "mean"),
    "max_AT_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "AT_mean", "max"),
    "min_AT_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "AT_mean", "min"),
    "time_min_AT_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "AT_mean", "time_min"),
    "sum_ST_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "ST_mean", "sum"),
    "mean_ST_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "ST_mean", "mean"),
    "max_ST_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "ST_mean", "max"),
    "min_ST_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "ST_mean", "min"),
    "time_min_ST_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "ST_mean", "time_min"),
    "sum_SM_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "SM_mean", "sum"),
    "mean_SM_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "SM_mean", "mean"),
    "max_SM_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "SM_mean", "max"),
    "min_SM_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "SM_mean", "min"),
    "time_min_SM_3_start": calculate_before_shrinking(df, event_ids, start_shrink, 3, "SM_mean", "time_min"),
    "sum_AT_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "AT_mean", "sum"),
    "mean_AT_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "AT_mean", "mean"),
    "max_AT_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "AT_mean", "max"),
    "min_AT_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "AT_mean", "min"),
    "time_min_AT_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "AT_mean", "time_min"),
    "sum_ST_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "ST_mean", "sum"),
    "mean_ST_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "ST_mean", "mean"),
    "max_ST_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "ST_mean", "max"),
    "min_ST_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "ST_mean", "min"),
    "time_min_ST_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "ST_mean", "time_min"),
    "sum_SM_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "SM_mean", "sum"),
    "mean_SM_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "SM_mean", "mean"),
    "max_SM_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "SM_mean", "max"),
    "min_SM_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "SM_mean", "min"),
    "time_min_SM_7_start": calculate_before_shrinking(df, event_ids, start_shrink, 7, "SM_mean", "time_min"),
    "sum_AT_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "AT_mean", "sum"),
    "mean_AT_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "AT_mean", "mean"),
    "max_AT_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "AT_mean", "max"),
    "min_AT_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "AT_mean", "min"),
    "time_min_AT_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "AT_mean", "time_min"),
    "sum_ST_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "ST_mean", "sum"),
    "mean_ST_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "ST_mean", "mean"),
    "max_ST_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "ST_mean", "max"),
    "min_ST_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "ST_mean", "min"),
    "time_min_ST_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "ST_mean", "time_min"),
    "sum_SM_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "SM_mean", "sum"),
    "mean_SM_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "SM_mean", "mean"),
    "max_SM_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "SM_mean", "max"),
    "min_SM_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "SM_mean", "min"),
    "time_min_SM_30_start": calculate_before_shrinking(df, event_ids, start_shrink, 30, "SM_mean", "time_min"),
    "sum_AT_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "AT_mean", "sum"),
    "mean_AT_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "AT_mean", "mean"),
    "max_AT_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "AT_mean", "max"),
    "min_AT_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "AT_mean", "min"),
    "time_min_AT_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "AT_mean", "time_min"),
    "sum_ST_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "ST_mean", "sum"),
    "mean_ST_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "ST_mean", "mean"),
    "max_ST_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "ST_mean", "max"),
    "min_ST_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "ST_mean", "min"),
    "time_min_ST_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "ST_mean", "time_min"),
    "sum_SM_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "SM_mean", "sum"),
    "mean_SM_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "SM_mean", "mean"),
    "max_SM_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "SM_mean", "max"),
    "min_SM_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "SM_mean", "min"),
    "time_min_SM_90_start": calculate_before_shrinking(df, event_ids, start_shrink, 90, "SM_mean", "time_min"),
    "sum_AT_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "AT_mean", "sum"),
    "mean_AT_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "AT_mean", "mean"),
    "sum_ST_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "ST_mean", "sum"),
    "mean_ST_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "ST_mean", "mean"),
    "sum_SM_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "SM_mean", "sum"),
    "mean_SM_365_start": calculate_before_shrinking(df, event_ids, start_shrink, 365, "SM_mean", "mean"),
    "sum_AT_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "AT_mean", "sum"),
    "mean_AT_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "AT_mean", "mean"),
    "sum_ST_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "ST_mean", "sum"),
    "mean_ST_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "ST_mean", "mean"),
    "sum_SM_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "SM_mean", "sum"),
    "mean_SM_1095_start": calculate_before_shrinking(df, event_ids, start_shrink, 1095, "SM_mean", "mean"),
    "sum_AT_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "AT_mean", "sum"),
    "mean_AT_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "AT_mean", "mean"),
    "max_AT_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "AT_mean", "max"),
    "min_AT_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "AT_mean", "min"),
    "time_min_AT_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "AT_mean", "time_min"),
    "sum_ST_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "ST_mean", "sum"),
    "mean_ST_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "ST_mean", "mean"),
    "max_ST_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "ST_mean", "max"),
    "min_ST_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "ST_mean", "min"),
    "time_min_ST_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "ST_mean", "time_min"),
    "sum_SM_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "SM_mean", "sum"),
    "mean_SM_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "SM_mean", "mean"),
    "max_SM_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "SM_mean", "max"),
    "min_SM_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "SM_mean", "min"),
    "time_min_SM_7_recovery": calculate_before_shrinking(df, event_ids, recovery, 7, "SM_mean", "time_min"),
    "sum_AT_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "AT_mean", "sum"),
    "mean_AT_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "AT_mean", "mean"),
    "max_AT_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "AT_mean", "max"),
    "min_AT_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "AT_mean", "min"),
    "time_min_AT_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "AT_mean", "time_min"),
    "sum_ST_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "ST_mean", "sum"),
    "mean_ST_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "ST_mean", "mean"),
    "max_ST_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "ST_mean", "max"),
    "min_ST_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "ST_mean", "min"),
    "time_min_ST_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "ST_mean", "time_min"),
    "sum_SM_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "SM_mean", "sum"),
    "mean_SM_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "SM_mean", "mean"),
    "max_SM_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "SM_mean", "max"),
    "min_SM_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "SM_mean", "min"),
    "time_min_SM_30_recovery": calculate_before_shrinking(df, event_ids, recovery, 30, "SM_mean", "time_min"),
    "sum_AT_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "sum"),
    "mean_AT_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "mean"),
    "max_AT_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "max"),
    "min_AT_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "min"),
    "time_min_AT_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "time_min"),
    "sum_ST_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "sum"),
    "mean_ST_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "mean"),
    "max_ST_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "max"),
    "min_ST_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "min"),
    "time_min_ST_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "time_min"),
    "sum_SM_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "sum"),
    "mean_SM_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "mean"),
    "max_SM_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "max"),
    "min_SM_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "min"),
    "time_min_SM_7_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "time_min"),
    "sum_AT_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "sum"),
    "mean_AT_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "mean"),
    "max_AT_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "max"),
    "min_AT_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "min"),
    "time_min_AT_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "AT_mean", "time_min"),
    "sum_ST_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "sum"),
    "mean_ST_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "mean"),
    "max_ST_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "max"),
    "min_ST_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "min"),
    "time_min_ST_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "ST_mean", "time_min"),
    "sum_SM_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "sum"),
    "mean_SM_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "mean"),
    "max_SM_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "max"),
    "min_SM_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "min"),
    "time_min_SM_3_stop": calculate_before_shrinking(df, event_ids, stop_shrink, 3, "SM_mean", "time_min")
})

shrinking_df = shrinking_df.reset_index()

print(shrinking_df.head())
shrinking_df.to_csv("input/shrinking-events-env.csv", index=False)
