import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy.stats import pearsonr
import dash
from dash import dcc, html
import numpy as np
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go


exec(open('src/constants/palettes.py').read())

df_long = pd.read_csv("input/shrinking-events-env-long.csv")

def met_season(dt):
    month = dt.month
    if month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"
    else:
        return "winter"
    
df_long["season"] = pd.to_datetime(df_long["start"]).apply(met_season)

def plot_correlation_by_window(df, func, timepoint, pct, corr_column, depth,
                                method='pearson', alpha_threshold=0.05):
    """
    Plot correlation coefficients between value and a specified column,
    grouped by window size (n) and environmental variable (env).
    
    Parameters:
    -----------
    df : DataFrame
        Long format dataframe with columns: env, n, func, timepoint, pct, value, and corr_column
    func : str
        Function type to filter (e.g., 'mean', 'sum', 'max', 'min', 'time')
    timepoint : str
        Timepoint to filter (e.g., 'start', 'stop', 'recovery')
    pct : bool
        Whether to use percentage values (True) or absolute values (False)
    corr_column : str
        Column name to correlate with value (e.g., 'shrink_days', 'total_days')
    depth : int
        Maximum depth to filter the data
    method : str, optional
        Correlation method ('pearson' or 'spearman'), default 'pearson'
    alpha_threshold : float, optional
        Significance threshold, default 0.05
    
    Returns:
    --------
    fig : plotly figure
        The generated plot
    results_df : DataFrame
        DataFrame with correlation coefficients and p-values
    """
    
    # Filter data
    df_filtered = df[
        (df['func'] == func) & 
        (df['timepoint'] == timepoint) & 
        (df['pct'] == pct) &
        (df['depth'] <= depth)
    ].copy()
    
    if df_filtered.empty:
        print(f"No data found for func={func}, timepoint={timepoint}, pct={pct}")
        return None, None
    
    # Remove rows with missing values in either column
    df_filtered = df_filtered.dropna(subset=['value', corr_column])
    
    # Calculate correlations for each env and n combination
    results = []
    
    for env in df_filtered['env'].unique():
        for n in sorted(df_filtered['n'].unique()):
            subset = df_filtered[(df_filtered['env'] == env) & (df_filtered['n'] == n)]
            
            if len(subset) > 2:  # Need at least 3 points for correlation
                if method == 'pearson':
                    corr, p_value = pearsonr(subset['value'], subset[corr_column])
                elif method == 'spearman':
                    from scipy.stats import spearmanr
                    corr, p_value = spearmanr(subset['value'], subset[corr_column])
                else:
                    raise ValueError("method must be 'pearson' or 'spearman'")
                
                results.append({
                    'env': env,
                    'n': n,
                    'correlation': corr,
                    'p_value': p_value,
                    'significant': p_value < alpha_threshold,
                    'n_samples': len(subset)
                })
    
    results_df = pd.DataFrame(results)
    
    if results_df.empty:
        print("No valid correlations could be calculated")
        return None, None
    
    # Create the plot
    fig = go.Figure()
    
    # Get unique n values and envs
    n_values = sorted(results_df['n'].unique())
    envs = sorted(results_df['env'].unique())
    
    # Calculate bar positions
    bar_width = 0.25
    n_envs = len(envs)
    
    # Plot bars for each env
    for i, env in enumerate(envs):
        env_data = results_df[results_df['env'] == env].sort_values('n')
        
        # Calculate x positions for this env
        offset = (i - (n_envs - 1) / 2) * bar_width
        x_positions = [n_values.index(n) + offset for n in env_data['n']]
        
        # Create separate traces for significant and non-significant
        sig_mask = env_data['significant'].values
        
        # Significant bars (solid)
        if sig_mask.any():
            sig_data = env_data[env_data['significant']]
            sig_x = [n_values.index(n) + offset for n in sig_data['n']]
            
            fig.add_trace(go.Bar(
                x=sig_x,
                y=sig_data['correlation'],
                name=env,
                marker_color=env_colors.get(env, 'gray'),
                opacity=0.9,
                width=bar_width,
                legendgroup=env,
                showlegend=True,
                hovertemplate=(
                    f'<b>{env}</b><br>' +
                    'Window: %{customdata[0]} days<br>' +
                    'Correlation: %{y:.3f}<br>' +
                    'P-value: %{customdata[1]:.4f}<br>' +
                    'N samples: %{customdata[2]}<br>' +
                    '<extra></extra>'
                ),
                customdata=np.column_stack((
                    sig_data['n'], 
                    sig_data['p_value'], 
                    sig_data['n_samples']
                ))
            ))
        
        # Non-significant bars (faded)
        if (~sig_mask).any():
            nonsig_data = env_data[~env_data['significant']]
            nonsig_x = [n_values.index(n) + offset for n in nonsig_data['n']]
            
            fig.add_trace(go.Bar(
                x=nonsig_x,
                y=nonsig_data['correlation'],
                name=f'{env} (ns)',
                marker_color=env_colors.get(env, 'gray'),
                opacity=0.3,
                width=bar_width,
                legendgroup=env,
                showlegend=False,
                hovertemplate=(
                    f'<b>{env}</b><br>' +
                    'Window: %{customdata[0]} days<br>' +
                    'Correlation: %{y:.3f}<br>' +
                    'P-value: %{customdata[1]:.4f} (ns)<br>' +
                    'N samples: %{customdata[2]}<br>' +
                    '<extra></extra>'
                ),
                customdata=np.column_stack((
                    nonsig_data['n'], 
                    nonsig_data['p_value'], 
                    nonsig_data['n_samples']
                ))
            ))
    
    # Add horizontal line at y=0
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)
    
    # Update layout
    fig.update_layout(
        title=dict(
            text=f'Correlation: {func} at {timepoint} {"(percentage)" if pct else "(absolute)"} vs {corr_column}',
            font=dict(size=16, weight='bold')
        ),
        xaxis=dict(
            title='Window Size (days)',
            tickmode='array',
            tickvals=list(range(len(n_values))),
            ticktext=n_values,
            title_font=dict(size=14, weight='bold')
        ),
        yaxis=dict(
            title=f'{method.capitalize()} Correlation Coefficient',
            title_font=dict(size=14, weight='bold'),
            gridcolor='lightgray',
            gridwidth=0.5
        ),
        barmode='group',
        bargap=0.15,
        bargroupgap=0.1,
        hovermode='closest',
        plot_bgcolor='white',
        legend=dict(
            title=dict(text='Environmental Variable'),
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        ),
        annotations=[
            dict(
                text=f'Transparent bars: p ≥ {alpha_threshold}',
                xref='paper',
                yref='paper',
                x=0.02,
                y=0.98,
                xanchor='left',
                yanchor='top',
                showarrow=False,
                bgcolor='wheat',
                bordercolor='black',
                borderwidth=1,
                borderpad=4,
                opacity=0.8
            )
        ],
        width=1000,
        height=600
    )
    
    return fig, results_df

# fig1, results1 = plot_correlation_by_window(
#     df_long, 
#     func='mean', 
#     timepoint='start', 
#     pct=False, 
#     corr_column='total_shrink',
#     depth=4
# )
    
# if fig1:
#     print("\nCorrelation Results:")
#     print(results1)
#     fig1.write_html('correlation_mean_start_shrinkdays.html')
#     fig1.show()

bgcolor = 'white'
textcolor = palette2[1]
linecolor = palette1[1]
shrinkingcolor = palette1[7]
growthcolor = palette1[3]

base_cols = ['shrink_days', 
             'total_days', 
             'start_doy',
             'end_doy',
             'recovery_doy',
             'total_shrink',
             'shrink_pct',
             'depth'
            ]

app = dash.Dash(__name__)

app.layout = html.Div([
    html.Label('Function:'),
    dcc.Dropdown(
        id='func-dropdown',
        options=[{'label': i, 'value': i} for i in sorted(df_long['func'].unique())],
        value=df_long['func'].unique()[0], 
        searchable=True,            
        placeholder="Select a function",
    ),
    html.Label('Timepoint:'),
    dcc.Dropdown(
        id='timepoint-dropdown',
        options=[{'label': i, 'value': i} for i in sorted(df_long['timepoint'].unique())],
        value=df_long['timepoint'].unique()[0], 
        searchable=True,            
        placeholder="Select a timepoint",
    ),
    html.Label('Correlation Column:'),
    dcc.Dropdown(
        id='column-dropdown',
        options=[{'label': i, 'value': i} for i in base_cols],
        value='shrink_pct',
        searchable=True,            
        placeholder="Select an attribute to correlate with",
    ),
    html.Label('Minimum Depth:'),
    dcc.Slider(
        id='depth-slider',
        min=0,
        max=int(df_long['depth'].max()),
        step=1,
        value=4,
        marks={i: str(i) for i in range(0, int(df_long['depth'].max())+1, 2)},
        tooltip={"placement": "bottom", "always_visible": True}
    ),
    html.Label('Total Shrinking Amount [μm]:'),
    dcc.RangeSlider(
        id='shrinking-slider',
        min=0,
        max=int(df_long['total_shrink'].max()),
        step=1,
        value=[int(df_long['total_shrink'].min()), int(df_long['total_shrink'].max())],
        marks={i: str(i) for i in range(0, int(df_long['total_shrink'].max())+1, 20)},
        tooltip={"placement": "bottom", "always_visible": True}
    ),
    html.Label('Percentage:'),
    dcc.RadioItems(
        id='pct-radio',
        options=[
            {'label': 'Absolute', 'value': False},
            {'label': 'Percentage', 'value': True}
        ],
        value=False,
        inline=True
    ),
    html.Label('Season of shrinking start:'),
    dcc.RadioItems(
        id='season-radio',
        options=[
            {'label': 'All', 'value': 'all'},
            {'label': 'Spring', 'value': 'spring'},
            {'label': 'Summer', 'value': 'summer'},
            {'label': 'Autumn', 'value': 'autumn'},
            {'label': 'Winter', 'value': 'winter'}
        ],
        value='all',
        inline=True
    ),
    dcc.Graph(id='bar-plot')
],
style={
    'backgroundColor': bgcolor,
    'color': textcolor,         
    'font-family': 'Arial, sans-serif',
    'padding': '20px'
})

@app.callback(
    dash.dependencies.Output('bar-plot', 'figure'),
    [dash.dependencies.Input('func-dropdown', 'value'),
     dash.dependencies.Input('timepoint-dropdown', 'value'),
     dash.dependencies.Input('column-dropdown', 'value'),
     dash.dependencies.Input('depth-slider', 'value'),
     dash.dependencies.Input('shrinking-slider', 'value'),
     dash.dependencies.Input('pct-radio', 'value'),
     dash.dependencies.Input('season-radio', 'value')]
)
def update_figure(selected_func, selected_timepoint, selected_column, selected_depth, selected_shrinking, selected_pct, selected_season):
    if selected_func is None or selected_timepoint is None:
        return go.Figure()

    selected_shrinking_min = selected_shrinking[0]
    selected_shrinking_max = selected_shrinking[1]
    
    dff = df_long[
        (df_long['total_shrink'] <= selected_shrinking_max) &
        (df_long['total_shrink'] >= selected_shrinking_min)
        ].copy()
    
    if selected_season != 'all':
        dff = dff[dff['season'] == selected_season]
        
    fig, results = plot_correlation_by_window(
        dff, 
        func=selected_func, 
        timepoint=selected_timepoint, 
        pct=selected_pct, 
        corr_column=selected_column,
        depth=selected_depth
    )
    
    if fig is None:
        return go.Figure()
        
    return fig

if __name__ == '__main__':
    app.run(debug=True)