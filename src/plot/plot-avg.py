import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

exec(open('src/constants/trend.py').read())
exec(open('src/constants/palettes.py').read())

df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["id", "date"])

df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month
df["doy"] = df["date"].dt.dayofyear
df["group"] = ""


def remove_initial(series):
    if series.notna().any():
        print('NotNA')
        return series - series.dropna().iloc[0]
    else:
        return pd.Series([float('nan')] * len(series), index=series.index)


irregular_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'irregular' in entry['trend']]
irregular_shrinking_ids = [entry['id'] for entry in irregular_shrinking]
df.loc[df['id'].isin(irregular_shrinking_ids), "group"] = "irregular_shrinking"

regular_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'regular' in entry['trend']]

winter_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'winter' in entry['season']]
shrinking_ids = [entry['id'] for entry in winter_shrinking]
df.loc[df['id'].isin(shrinking_ids), "group"] = "winter_shrinking"

summer_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'summer' in entry['season']]
shrinking_ids = [entry['id'] for entry in summer_shrinking]
df.loc[df['id'].isin(shrinking_ids), "group"] = "summer_shrinking"

spring_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'spring' in entry['season']]
shrinking_ids = [entry['id'] for entry in spring_shrinking]
df.loc[df['id'].isin(shrinking_ids), "group"] = "spring_shrinking"

autumn_shrinking = [entry for entry in trend if 'shrinking' in entry['trend'] and 'autumn' in entry['season']]
shrinking_ids = [entry['id'] for entry in autumn_shrinking]
df.loc[df['id'].isin(shrinking_ids), "group"] = "autumn_shrinking"

dormancy = [entry for entry in trend if 'dormancy' in entry['trend']]
dormancy_ids = [entry['id'] for entry in dormancy]
df.loc[df['id'].isin(dormancy_ids), "group"] = "dormancy"

def plot_lines(df, group_value, date_col="date", value_col="normalized"):
    """
    Create a line chart for a given group.

    Parameters
    ----------
    df : pd.DataFrame
        A long-format dataframe with 'datetime', 'group', and values.
    group_value : str
        Which group to plot (e.g. 'winter_shrinking').
    value_col : str
        Which value column to plot (default: 'normalized').
    """
    dff = df[df["group"] == group_value].copy()
    if dff.empty:
        print(f"No data available for group: {group_value}")
        return
    dff["trend_shift"] = (dff["trend"] != dff["trend"].shift()) | (dff[date_col].diff().dt.days != 1)
    dff["segment"] = dff["trend_shift"].cumsum()

    color_map = {
        "shrinking": palette1[7],
        "growth": palette1[2],
    }
    
    bgcolor = 'white'
    textcolor = palette1[1]

    fig = go.Figure()
    for (trend, segment), seg_df in dff.groupby(["trend", "segment"]):
        fig.add_trace(go.Scatter(
            x=seg_df[date_col],
            y=seg_df[value_col],
            mode="lines",
            name=f"{trend} segment {segment}",
            line=dict(color=color_map.get(trend, "black")),
            hovertext=seg_df["id"],
            showlegend=False,
        ))
    for trend, color in color_map.items():
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode="lines",
            line=dict(color=color),
            name=trend
        ))
    fig.update_layout(
        title=f"{group_value}",
        xaxis_title="Date",
        yaxis_title="Stem diameter change [μm]",
        plot_bgcolor=bgcolor,   
        paper_bgcolor=bgcolor,  
        font_color=textcolor,    
        title_font_size=20,
        template="plotly_white"
    )
    fig.update_xaxes(
        dtick="M12",        
        showgrid=True,
        gridwidth=1,
        gridcolor=textcolor,
        tickformat="%Y"     
    )
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=textcolor)
    fig.show(renderer="browser")
    
plot_lines(df, "winter_shrinking")
plot_lines(df, "summer_shrinking")
plot_lines(df, "irregular_shrinking")
plot_lines(df, "dormancy")

def plot_one_year(df, year, group_value="all", date_col="doy", value_col="normalized"):
    if group_value != "all":
        dff = df[(df["year"] == year) & (df["group"] == group_value) & (df["trend"] == "shrinking")].copy()
    else:
        dff = df[(df["year"] == year) & (df["trend"] == "shrinking")].copy()
    if dff.empty:
        print(f"No data available for year: {year}")
        return
    
    dff["value"] = dff.groupby("id")[value_col].transform(remove_initial)
    print(dff[["id", "value"]].drop_duplicates().head())

    bgcolor = 'white'
    textcolor = palette1[1]
    line_color = palette1[2] 

    fig = go.Figure()
    for id_val, group in dff.groupby("id"):
        fig.add_trace(go.Scatter(
            x=group[date_col], 
            y=group["value"],
            mode="lines",
            line=dict(color=line_color),
            name=str(id_val),
            hovertext=group["id"],
            showlegend=False  
        )) 
        
    fig.update_layout(
        title=f"{year} " + f"{group_value}",
        xaxis_title="Day of Year",
        yaxis_title="Stem diameter change [μm]",
        plot_bgcolor=bgcolor,   
        paper_bgcolor=bgcolor,  
        font_color=textcolor,    
        title_font_size=20,
        template="plotly_white",
        showlegend=False
    )
    fig.update_xaxes(
        dtick=30,  
        showgrid=True,
        gridwidth=1,
        gridcolor=textcolor,
        range=[1, 366],  
        title="Day of Year"
    )
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=textcolor)
    fig.show(renderer="browser")
    
plot_one_year(df, 2021, "winter_shrinking")

plot_one_year(df, 2021, "all")
plot_one_year(df, 2017, "all")
plot_one_year(df, 2013, "all")
plot_one_year(df, 2021, "irregular_shrinking")

