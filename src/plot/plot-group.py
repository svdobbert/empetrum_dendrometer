import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

exec(open('src/constants/trend.py').read())
exec(open('stc/constants/palettes.py').read())

df = pd.read_csv("input/data_with_trends.csv")
df["date"] = pd.to_datetime(df["datetime"])
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

# group
# groups: summer_shrinking, summer_shrinking_2013, winter_shrinking_2017, winter_shrinking_2021, dormancy
summer_shrinking= [entry for entry in trend if 'summer_shrinking' in entry['group']]
summer_shrinking_ids = [entry['id'] for entry in summer_shrinking]
summer_shrinking_df = df[df['id'].isin(summer_shrinking_ids)]

summer_shrinking_2013 = [entry for entry in trend if 'summer_shrinking_2013' in entry['group']]
summer_shrinking_2013_ids = [entry['id'] for entry in summer_shrinking_2013]
summer_shrinking_2013_df = df[df['id'].isin(summer_shrinking_2013_ids)]

winter_shrinking_2017 = [entry for entry in trend if 'winter_shrinking_2017' in entry['group']]
winter_shrinking_2017_ids = [entry['id'] for entry in winter_shrinking_2017]
winter_shrinking_2017_df = df[df['id'].isin(winter_shrinking_2017_ids)]

winter_shrinking_2021 = [entry for entry in trend if 'winter_shrinking_2021' in entry['group']]
winter_shrinking_2021_ids = [entry['id'] for entry in winter_shrinking_2021]
winter_shrinking_2021_df = df[df['id'].isin(winter_shrinking_2021_ids)]

dormancy = [entry for entry in trend if 'dormancy' in entry['group']]
dormancy_ids = [entry['id'] for entry in dormancy]
dormancy_df = df[df['id'].isin(dormancy_ids)]

def plot_one_year_avg(df, year, date_col="date", value_col="normalized"):
    dff = df[(df["year"] == year)].copy()

    dff["value"] = dff.groupby("id")[value_col].transform(remove_initial)
    print(dff[["id", "value"]].drop_duplicates().head())

    bgcolor = 'white'
    textcolor = palette2[1]
    line_color = palette2[2] 

    fig = go.Figure()
    id_list = dff["id"].unique()
    custom_palette = palette2[:len(id_list)]  

    id_color_map = {id_val: custom_palette[i % len(custom_palette)] for i, id_val in enumerate(id_list)}

    for id_val, group in dff.groupby("id"):
        fig.add_trace(go.Scatter(
            x=group[date_col], 
            y=group["value"],
            mode="lines",
            hovertext=group["id"],
            line=dict(color=id_color_map[id_val]),
            name=str(id_val),
            showlegend=True,
            legendgroup=str(id_val),
            visible="legendonly"
        )) 

    avg_df = dff.groupby(date_col)["value"].mean().reset_index()

    window = 7  # days
    avg_df["smoothed"] = avg_df["value"].rolling(window, min_periods=1, center=True).mean()

    fig.add_trace(go.Scatter(
        x=avg_df[date_col],
        y=avg_df["smoothed"],
        mode="lines",
        line=dict(color=color, width=4),
        name="Average",
        legendgroup="Average",        
        showlegend=True,    
        hoverinfo="skip",
    ))

    if "AT" in dff.columns:
        at_df = dff.groupby(date_col)["AT"].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=at_df[date_col],
            y=at_df["AT"],
            mode="lines",
            line=dict(color=color, width=3, dash="dot"),
            name="Average " + "AT",
            legendgroup="Average",            
            showlegend=True, 
            yaxis="y2"
        ))
        for id_val, group in dff.groupby("id"):
            fig.add_trace(go.Scatter(
                x=group[date_col], 
                y=group["AT"],
                mode="lines",
                hovertext=group["id"],
                line=dict(color=id_color_map[id_val], width=2, dash="dot"),
                name=str(id_val) + " " + "AT",        
                showlegend=True,
                yaxis="y2",
                legendgroup=str(id_val),
                visible="legendonly"
            )) 
            
    if "ST" in dff.columns:
        at_df = dff.groupby(date_col)["ST"].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=at_df[date_col],
            y=at_df["ST"],
            mode="lines",
            line=dict(color=color, width=3, dash="dash"),
            name="Average " + "ST",
            legendgroup="Average",            
            showlegend=True, 
            yaxis="y2"
        ))
        for id_val, group in dff.groupby("id"):
            fig.add_trace(go.Scatter(
                x=group[date_col], 
                y=group["ST"],
                mode="lines",
                hovertext=group["id"],
                line=dict(color=id_color_map[id_val], width=2, dash="dash"),
                name=str(id_val) + " " + "ST",        
                showlegend=True,
                yaxis="y2",
                legendgroup=str(id_val),
                visible="legendonly"
            )) 
            
    if "SM" in dff.columns:
        at_df = dff.groupby(date_col)["SM"].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=at_df[date_col],
            y=at_df["SM"],
            mode="lines",
            line=dict(color=color, width=3, dash="longdash"),
            name="Average " + "SM",
            legendgroup="Average",            
            showlegend=True, 
            yaxis="y3"
        ))
        for id_val, group in dff.groupby("id"):
            fig.add_trace(go.Scatter(
                x=group[date_col], 
                y=group["SM"],
                mode="lines",
                hovertext=group["id"],
                line=dict(color=id_color_map[id_val], width=2, dash="longdash"),
                name=str(id_val) + " " + "SM",        
                showlegend=True,
                yaxis="y3",
                legendgroup=str(id_val),
                visible="legendonly"
            )) 

    fig.update_layout(
        title=f"{year}",
        xaxis_title="",
        yaxis_title="Stem diameter change [μm]",
        legend=dict(groupclick="toggleitem"),
        plot_bgcolor=bgcolor,   
        paper_bgcolor=bgcolor,  
        font_color=textcolor,    
        title_font_size=20,
        template="plotly_white",
        showlegend=True,
        yaxis=dict(
            title="Stem diameter change [μm]",
            side="left"
        ),
        yaxis2=dict(
            title="Temperature [°C]",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        yaxis3=dict(
            title="Soil moisture [m³/m³]",
            overlaying="y",
            side="right",
            position=0.99,
            showgrid=False
        ),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=textcolor
    )
    fig.update_yaxes(showgrid=False, gridwidth=1, gridcolor=textcolor)
    fig.show(renderer="browser")
    fig.write_html(f"output/one-year-{year}.html")

plot_one_year_avg(summer_shrinking_2013_df, 2013)
plot_one_year_avg(summer_shrinking_2013_df, 2013)
plot_one_year_avg(summer_shrinking_2013_df, 2013)

plot_one_year_avg(winter_shrinking_2017_df, 2017)

plot_one_year_avg(winter_shrinking_2021_df, 2021)


