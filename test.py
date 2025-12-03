import pandas as pd
from datetime import timedelta

df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["week"] = df["date"].dt.isocalendar().week
df["month"] = df["date"].dt.month
df["doy"] = df["date"].dt.dayofyear
df["group"] = df["id"] + " " + df["year"].astype(str) + " " + df["week"].astype(str)
df = df.dropna(subset=["D_mean"])

first_val = df.groupby("id")["D_mean"].transform(lambda s: s.dropna().iloc[0] if s.notna().any() else float("nan"))
df["D_base"] = df["D_mean"] - first_val

def detect_shrinking_events(
    df,
    drop_threshold_pct=0.05,      # % of amplitude required to trigger shrinking
    rise_threshold_pct=0.02,      # % of amplitude required to consider rising
    max_gap_days=2,               # allowed small positive intervals inside shrinking
    max_positive_run=1,           # allowed consecutive positive days
):
    """
    Detect shrinking events inside a single-ID DataFrame.
    DataFrame must contain: date (datetime64) and D_mean.
    """

    g = df.sort_values("date").reset_index(drop=True)

    # amplitude for this ID
    amp = g["D_mean"].max() - g["D_mean"].min()

    # thresholds
    drop_thr = drop_threshold_pct * amp
    rise_thr = rise_threshold_pct * amp

    # daily change
    g["diff"] = g["D_mean"].diff()

    events = []
    in_event = False
    start_idx = None
    last_negative_day = None
    positive_streak = 0
    
    for i in range(1, len(g)):
        d = g.loc[i, "diff"]

        # Case 1: we are NOT yet inside an event → check for start
        if not in_event:
            # condition: cumulative drop from the local max
            window = g.loc[:i, "D_mean"]
            local_max = window.max()
            cumulative_drop = local_max - g.loc[i, "D_mean"]

            if cumulative_drop >= drop_thr:
                in_event = True
                start_idx = i
                last_negative_day = g.loc[i, "date"]
                positive_streak = 0
            continue

        # Case 2: we ARE inside an event
        if d < 0:
            # negative day → update trackers
            last_negative_day = g.loc[i, "date"]
            positive_streak = 0
        else:
            # positive day
            positive_streak += 1

        # Check if event continues
        gap_days = (g.loc[i, "date"] - last_negative_day).days

        still_shrinking = (
            gap_days <= max_gap_days and positive_streak <= max_positive_run
        )

        if still_shrinking:
            continue

        # Check if it rose enough to end shrinking period
        window = g.loc[start_idx:i, "D_mean"]
        local_min = window.min()
        rise_amount = g.loc[i, "D_mean"] - local_min

        if rise_amount >= rise_thr:
            events.append(
                {
                    "start": g.loc[start_idx, "date"],
                    "end": g.loc[i, "date"],
                    "drop_amount": local_max - local_min if "local_max" in locals() else None,
                }
            )
            in_event = False
            start_idx = None

    # close last event if needed
    if in_event:
        end_idx = len(g) - 1
        window = g.loc[start_idx:end_idx, "D_mean"]
        local_min = window.min()
        local_max = g.loc[:start_idx, "D_mean"].max()
        events.append(
            {
                "start": g.loc[start_idx, "date"],
                "end": g.loc[end_idx, "date"],
                "drop_amount": local_max - local_min,
            }
        )

    return pd.DataFrame(events)


def classify_outer_inner(event_df):
    """
    Adds columns:
    - type: 'outer' or 'inner'
    - outer_event_id: grouping variable
    """

    if event_df.empty:
        return event_df

    event_df = event_df.sort_values(["start", "end"]).reset_index(drop=True)

    outer_id = 0
    event_df["type"] = None
    event_df["outer_event_id"] = None

    for i in range(len(event_df)):
        s_i, e_i = event_df.loc[i, ["start", "end"]]

        # check if contained
        containing = [
            j for j in range(len(event_df))
            if j != i
            and event_df.loc[j, "start"] <= s_i
            and event_df.loc[j, "end"] >= e_i
        ]

        if containing:
            # inner event
            parent = containing[0]
            event_df.loc[i, "type"] = "inner"
            event_df.loc[i, "outer_event_id"] = event_df.loc[parent, "outer_event_id"]
        else:
            # new outer event
            event_df.loc[i, "type"] = "outer"
            event_df.loc[i, "outer_event_id"] = outer_id
            outer_id += 1

    return event_df


def process_all_ids(df, **kwargs):
    """
    Apply shrinking event detection to every ID in the dataset.
    """

    all_events = []

    for id_, g in df.groupby("id"):
        ev = detect_shrinking_events(g, **kwargs)
        if ev.empty:
            continue

        ev = classify_outer_inner(ev)
        ev["id"] = id_
        ev["start_doy"] = ev["start"].dt.dayofyear
        ev["end_doy"] = ev["end"].dt.dayofyear

        all_events.append(ev)

    if not all_events:
        return pd.DataFrame()

    return pd.concat(all_events, ignore_index=True)

events = process_all_ids(
    df,
    drop_threshold_pct=0.1,  # shrinking begins after 3% amplitude drop
    rise_threshold_pct=0.1,  # ends when rising exceeds 2% of amplitude
    max_gap_days=2,           # allow 2 days gap between negatives
    max_positive_run=4        # allow 1 day of positive change inside event
)

print(events)
events.to_csv("output/shrinking-events.csv", index=False)