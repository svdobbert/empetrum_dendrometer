import pandas as pd
import plotly.graph_objects as go

exec(open('src/constants/palettes.py').read())

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

min_weekly_change = weekly_total_change[weekly_total_change["change"] < -40]

result = []

for _, row in min_weekly_change.iterrows():
    id_val = row["id"]
    year_val = row["year"]
    week_val = row["week"]
    week_df = df[(df["id"] == id_val) & (df["year"] == year_val) & (df["week"] == week_val)]

    if not week_df.empty and week_df["change"].notna().any():
        min_daily_row = week_df.loc[week_df["change"].idxmin()]
        result.append({
            "id": id_val,
            "year": year_val,
            "week": week_val,
            "min_weekly_change": row["change"],
            "date_of_min_daily_change": min_daily_row["date"],
            "min_daily_change": min_daily_row["change"]
        })
    else:
        print(f"Warning: No data for id={id_val}, year={year_val}, week={week_val}")

result_df = pd.DataFrame(result)
result_df.to_csv("input/shrinking-time.csv", index=False)
print(result_df)

# Calculate cumulative change for each id
df["cum_change"] = df.groupby("id")["change"].transform("cumsum")

first_val = df.groupby("id")["D_mean"].transform(lambda s: s.dropna().iloc[0] if s.notna().any() else float("nan"))
df["D_base"] = df["D_mean"] - first_val

df["cum_pos_change"] = df.groupby("id")["D_base"].transform("cummax") 

df["cum_neg_change"] = df.groupby("id")["D_base"].transform("cummin")

df.to_csv("input/daily_data_with_cumulative.csv", index=False)

bgcolor = 'white'
textcolor = palette2[1]
    
fig = go.Figure()
for id_val, g in df.groupby("id"):
    fig.add_trace(go.Scatter(
        x=g["date"],
        y=g["D_base"],          
        mode="lines",
        name=str(id_val),
        showlegend=True,
        legendgroup=str(id_val),
        visible="legendonly"
    ))
    fig.add_trace(go.Scatter(
        x=g["date"],
        y=g["cum_neg_change"],          
        mode="lines",
        name=str(id_val) + " " + "cum shrink",        
        showlegend=True,
        legendgroup=str(id_val),
        visible="legendonly"
    ))
    fig.add_trace(go.Scatter(
        x=g["date"],
        y=g["cum_pos_change"],          
        mode="lines",
        name=str(id_val) + " " + "cum growth",        
        showlegend=True,
        legendgroup=str(id_val),
        visible="legendonly"
    ))
fig.update_layout(
    title=f"Cummulative stem diameter change per individuum",
    xaxis_title="",
    yaxis_title="Stem diameter change [μm]",
    legend=dict(groupclick="toggleitem"),
    plot_bgcolor=bgcolor,   
    paper_bgcolor=bgcolor,  
    font_color=textcolor,    
    title_font_size=20,
    template="plotly_white",
    showlegend=True,
)
fig.update_xaxes(
    showgrid=True,
    gridcolor=textcolor
)
fig.update_yaxes(showgrid=False, gridwidth=1, gridcolor=textcolor)
fig.show(renderer="browser")
fig.write_html(f"output/cummulative-curves.html")
