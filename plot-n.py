import plotly.express as px
import pandas as pd

df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df["year"] = df["date"].dt.year.astype(int)

df_unique = df.drop_duplicates(subset=["id", "year"])[["id", "year", "trend"]]
shrinking_counts = df_unique[df_unique["trend"] == "shrinking"].groupby("year").size()
shrinking_counts.name = "shrinking_count"

growth_counts = df_unique[df_unique["trend"] == "growth"].groupby("year").size()

total_counts = df_unique.groupby("year").size()
total_counts.name = "total_count"

stats = pd.concat([total_counts, shrinking_counts], axis=1).fillna(0)
stats["growth_count"] = growth_counts
stats["growth_count"] = stats["growth_count"].fillna(0)

stats["shrinking_pct"] = stats["shrinking_count"] / stats["total_count"] * 100

stats = stats.reset_index()
print(stats)

import plotly.express as px

fig = px.bar(stats, x="year", y="shrinking_count",
             text="shrinking_pct",
             labels={"shrinking_count": "Number of Shrinking IDs", "year": "Year"},
             title="Shrinking IDs per Year")
fig.show()