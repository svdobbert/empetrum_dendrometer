import dash
from dash import dcc, html
import numpy as np
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
from scipy.interpolate import UnivariateSpline
from datetime import datetime

exec(open('constants/palettes.py').read())

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

first_val_year = df.groupby(["id", "year"])["D_mean"].transform(lambda s: s.dropna().iloc[0] if s.notna().any() else float("nan"))
df["D_base_year"] = df["D_mean"] - first_val_year

df["cum_pos_change"] = df.groupby("id")["D_base"].transform("cummax")
df["cum_pos_change_year"] = df.groupby(["id", "year"])["D_base_year"].transform("cummax")
df["cum_neg_change"] = df.groupby("id")["D_base"].transform("cummin")
df["cum_neg_change_year"] = df.groupby(["id", "year"])["D_base_year"].transform("cummin")

bgcolor = 'white'
textcolor = palette2[1]
linecolor = palette1[1]
shrinkingcolor = palette1[7]
growthcolor = palette1[3]

app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(
        id='id-dropdown',
        options=[{'label': i, 'value': i} for i in df['id'].unique()],
        value=df['id'].unique()[0], 
        searchable=True,            
        placeholder="Select an ID",
    ),
    dcc.Dropdown(
        id='year-dropdown',
        options=[{'label': 'All', 'value': 'All'}] + [{'label': i, 'value': i} for i in df['year'].unique()],
        value=df['year'].unique()[0], 
        searchable=True,            
        placeholder="Select an ID",
    ),
    dcc.Graph(id='line-plot')
],
style={
    'backgroundColor': bgcolor,
    'color': textcolor,         
    'font-family': 'Arial, sans-serif',
    'padding': '0px'
})

@app.callback(
    dash.Output('line-plot', 'figure'),
    dash.Input('id-dropdown', 'value'),
    dash.Input('year-dropdown', 'value')
)
def update_figure(selected_id, selected_year, date_col="date", value_col="D_base"):
    if selected_id is None:
        return go.Figure()
    
    try:
        sel_year = int(selected_year) if selected_year is not None else None
    except Exception:
        sel_year = selected_year

    if sel_year is None or sel_year == "All":
        dff = df[df['id'] == selected_id].copy()
        neg_col = 'cum_neg_change'
        pos_col = 'cum_pos_change'     
    else:
        dff = df[(df['id'] == selected_id) & (df['year'] == sel_year)].copy()
        neg_col = 'cum_neg_change_year'
        pos_col = 'cum_pos_change_year'
        value_col = "D_base_year"   

    if dff.empty:
        return go.Figure()
    
    
    fig = px.line(
        dff,
        x=date_col,
        y=value_col,
        title=str(dff["site"].iloc[0]) + "." + str(dff["individuum"].iloc[0]),
        labels={date_col: "Date", value_col: "Stem diameter change [μm]"},
        color_discrete_sequence=[linecolor],
        line_dash_sequence=["solid"],   
        markers=False          
    )   
    fig.add_trace(go.Scatter(
        x=dff[date_col],
        y=dff[neg_col],
        mode="lines",
        showlegend=False,
        line=dict(color=shrinkingcolor, width=12, dash="dash"),
        hoverinfo="skip"
    ))
    fig.add_trace(go.Scatter(
        x=dff[date_col],
        y=dff[pos_col],
        mode="lines",
        showlegend=False,
        line=dict(color=growthcolor, width=12, dash="dash"),
        hoverinfo="skip"
    ))
    
    fig.update_layout(
        height=800,
        width=1800,
        plot_bgcolor=bgcolor,   
        paper_bgcolor=bgcolor,  
        font_color=textcolor,    
        title_font_size=20,
        template="plotly_white"
    )
    
    fig.update_xaxes(
    showgrid=True,
    gridcolor=textcolor,
    tickformat="%Y-%m"     
    )
    
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=textcolor)
    fig.update_traces(line=dict(width=2.5))     
    return fig

if __name__ == '__main__':
    app.run(debug=True)
