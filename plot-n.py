
import pandas as pd
import plotly.express as px

exec(open('constants/palettes.py').read())

def plot_shrinking_vs_growth(df, group_col=None, palette_shrinking=palette1[2], palette_growth=palette1[4]):
    """
    Create a 100% stacked bar chart showing shrinking vs growth per year, optionally grouped by another column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with at least ['id', 'year', 'trend'] columns.
    group_col : str or None
        Column to group by (e.g., "site"). If None, aggregates across all IDs.
    palette_shrinking : str
        Color for shrinking portion.
    palette_growth : str
        Color for growth portion.

    Returns
    -------
    fig : plotly.graph_objs._figure.Figure
    """
    df["year"] = df["date"].dt.year.astype(int)

    subset_cols = ["id", "year", "trend"]
    if group_col is not None:
        subset_cols.append(group_col)

    df_unique = df.drop_duplicates(subset=subset_cols)

    if group_col is None:
        total_counts = df_unique.groupby("year").size().rename("total_count")
        shrinking_counts = df_unique[df_unique["trend"] == "shrinking"].groupby("year").size().rename("shrinking_count")
        growth_counts = df_unique[df_unique["trend"] == "growth"].groupby("year").size().rename("growth_count")
        stats = pd.concat([total_counts, shrinking_counts, growth_counts], axis=1).fillna(0)
        stats["shrinking_pct"] = stats["shrinking_count"] / stats["total_count"] * 100
        stats["growth_pct"] = 100 - stats["shrinking_pct"]
        stats = stats.reset_index()
        stats[group_col] = "All"
    else:
        total_counts = df_unique.groupby(["year", group_col]).size().rename("total_count")
        shrinking_counts = df_unique[df_unique["trend"] == "shrinking"].groupby(["year", group_col]).size().rename("shrinking_count")
        growth_counts = df_unique[df_unique["trend"] == "growth"].groupby(["year", group_col]).size().rename("growth_count")
        stats = pd.concat([total_counts, shrinking_counts, growth_counts], axis=1).fillna(0)
        stats["shrinking_pct"] = stats["shrinking_count"] / stats["total_count"] * 100
        stats["growth_pct"] = 100 - stats["shrinking_pct"]
        stats = stats.reset_index()

    df_long = stats.melt(
        id_vars=["year", "total_count"] + ([group_col] if group_col else []),
        value_vars=["shrinking_pct", "growth_pct"],
        var_name="trend",
        value_name="percentage"
    )


    count_map = {
        "shrinking_pct": "shrinking_count",
        "growth_pct": "growth_count"
    }
    
    df_long["count"] = df_long.apply(
        lambda row: stats.loc[
            (stats["year"] == row["year"]) &
            ((stats[group_col] == row[group_col]) if group_col else True),
            count_map[row["trend"]]
        ].values[0],
        axis=1
    )

    
    df_long["trend_label"] = df_long["trend"].replace({
        "shrinking_pct": "Shrinking",
        "growth_pct": "Growth"
    })

    df_long["percentage_rounded"] = df_long["percentage"].round(1)


    fig = px.bar(
        df_long,
        x="year",
        y="percentage",
        color="trend_label",
        text="percentage_rounded",
        facet_col=group_col if group_col else None,
        color_discrete_map={"Shrinking": palette_shrinking, "Growth": palette_growth},
        labels={"percentage": "Percentage of Individuals [%]", "year": "Year", "trend_label": ""},
        hover_data=["count", "total_count"],
        title=f"Shrinking vs Growth per Year{f' by {group_col}' if group_col else ''}"
    )

    fig.update_layout(
        barmode="stack",
        font_color="#000000",
        title_font_size=20,
        template="plotly_white"
    )

    return fig

df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["D_mean"])

split_cols = df["id"].str.split(".", expand=True)
split_cols.columns = ["country", "site", "individuum", "branch", "section", "radius"]
df = pd.concat([df[["date", "year", "D_mean", "id", "trend"]], split_cols[["site"]]], axis=1)
df["position"] =  df["site"].str[-1]
df["region"] =  df["site"].str[0]

df["position_str"] = df["position"].replace({
        "A": "ridge",
        "I": "ridge",
        "J": "ridge",
        "C": "slope",
        "D": "slope",
        "M": "slope",
        "N": "slope",
        "E": "slope",
        "F": "slope",
        "Z": "slope",
        "B": "depression",
        "V": "depression", 
        "U": "depression"
    }).astype("category")

print(df["id"].unique())

fig = plot_shrinking_vs_growth(df)
fig.show(renderer="browser")
fig.write_html("output/shrinking-vs-growth-all.html")


fig = plot_shrinking_vs_growth(df, group_col="position_str")
fig.show(renderer="browser")
fig.write_html("output/shrinking-vs-growth-position.html")

fig = plot_shrinking_vs_growth(df, group_col="region")
fig.show(renderer="browser")
fig.write_html("output/shrinking-vs-growth-region.html")

