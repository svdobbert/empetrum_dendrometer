import dash
from dash import dcc, html
import numpy as np
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
from scipy.interpolate import UnivariateSpline
from datetime import datetime

exec(open('src/constants/palettes.py').read())


def plot_id_line(df, id_value, date_col="date", value_col="D_mean"):
    """
    Create a line chart for a given id.

    Parameters
    ----------
    df : pd.DataFrame
        A long-format dataframe with 'datetime', 'id', and values.
    id_value : str
        Which id to plot (e.g. 'W0770A').
    value_col : str
        Which value column to plot (default: 'D_mean').
    """
    dff = df[df["id"] == id_value].copy()

    fig = px.line(
        dff,
        x=date_col,
        y=value_col,
        title=str(dff["site"].iloc[0]) + "." + str(dff["individuum"].iloc[0]),
        labels={date_col: "Date", value_col: "Stem diameter change [μm]"}
    )
    fig.show(renderer="browser")

df = pd.read_csv("input/daily_data_with_trends.csv")
df["date"] = pd.to_datetime(df["date"])
df["year"] = df["date"].dt.year.astype(int)
bgcolor = 'white'
textcolor = palette1[1]
shrinkingcolor = palette1[2]
growthcolor = palette1[4]
linecolor = palette1[1]
smoothcolor = palette1[6]


app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Dropdown(
        id='id-dropdown',
        options=[{'label': i, 'value': i} for i in df['id'].unique()],
        value=df['id'].unique()[0], 
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
    dash.Input('id-dropdown', 'value')
)
def update_figure(selected_id, date_col="date", value_col="D_mean"):
    dff = df[df['id'] == selected_id]
    shrinking_years = dff[dff["trend"] == "shrinking"]["year"].unique()
    growing_years = dff[dff["trend"] == "growth"]["year"].unique()
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
    
    dff_fit = dff[[date_col, value_col]].dropna().copy()
    if dff_fit.shape[0] < 4:
        return fig

    dff_fit[date_col] = pd.to_datetime(dff_fit[date_col])
    dff_fit = dff_fit.sort_values(date_col)

    dates = dff_fit[date_col]
    x = (dates - dates.min()).dt.total_seconds() / 86400.0  
    y = dff_fit[value_col].astype(float).values

    var_y = np.var(y) if np.var(y) > 0 else 0.0
    s_default = max(0.0, len(x) * var_y * 0.01)   
    spline = UnivariateSpline(x, y, s=s_default)

    x_smooth = np.linspace(x.min(), x.max(), 500)
    y_smooth = spline(x_smooth)

    x_smooth_dt = dates.min() + pd.to_timedelta(x_smooth, unit="D")
    x_smooth_dt = pd.to_datetime(x_smooth_dt) 

    fig.add_trace(
        go.Scatter(
            x=x_smooth_dt,
            y=y_smooth,
            mode="lines",
            name="Spline fit",
            opacity=0.5,     
            line=dict(color=smoothcolor, width=12),
            hoverinfo="skip"   
        )
    )
    
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
        dtick="M12",        
        showgrid=True,
        gridwidth=1,
        gridcolor=textcolor,
        tickformat="%Y"     
    )
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=textcolor)
    fig.update_traces(line=dict(width=2.5)) 
    
    for yr in shrinking_years:
        start = pd.Timestamp(f"{yr}-01-01")
        end = pd.Timestamp(f"{yr}-12-31")
    
        fig.add_vrect(
            x0=start,
            x1=end,
            fillcolor=shrinkingcolor,      
            opacity=0.4,          
            layer="below",        
            line_width=0
        )  
        
    for yr in growing_years:
        start = pd.Timestamp(f"{yr}-01-01")
        end = pd.Timestamp(f"{yr}-12-31")
    
        fig.add_vrect(
            x0=start,
            x1=end,
            fillcolor=growthcolor,      
            opacity=0.2,          
            layer="below",        
            line_width=0
        )   
    
    return fig

if __name__ == '__main__':
    app.run(debug=True)
