import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import plotly.express as px


exec(open('src/constants/palettes.py').read())

df_events = pd.read_csv("input/shrinking-events.csv")
df_events_env = pd.read_csv("input/shrinking-events-env-long.csv")

# correlation matrix
input = df_events[df_events["depth"] <= 4].copy()
numeric_cols = input.select_dtypes(include=[np.number]).columns.tolist()
selected_cols = ["shrink_days", "total_days", "start_doy", "end_doy", "recovery_doy", "total_shrink", "shrink_pct", "depth"]
numeric_cols = [c for c in numeric_cols if c.lower() in selected_cols]
x_vars = numeric_cols
y_vars = numeric_cols
corr_mat = pd.DataFrame(np.nan, index=y_vars, columns=x_vars)
pval_mat = pd.DataFrame(np.nan, index=y_vars, columns=x_vars)

for yi in y_vars:
    for xi in x_vars:
        x = input[xi]
        y = input[yi]
        mask = x.notna() & y.notna()
        if mask.sum() >= 2:
            r, p = pearsonr(x[mask], y[mask])
            corr_mat.loc[yi, xi] = r
            pval_mat.loc[yi, xi] = p

alpha = 0.05
corr_display = corr_mat.mask(pval_mat > alpha)
corr_display = corr_display.round(2)
print(corr_display)

fig = px.imshow(
    corr_display.T,
    x=corr_display.T.columns,
    y=corr_display.T.index,
    color_continuous_scale="RdBu_r",
    zmin=-1, zmax=1,
    text_auto=True,
    aspect="auto",
    title=f"Cross-correlation",
    template="plotly_white"
)
fig.update_layout(width=800, height=800)
fig.show(renderer="browser")
fig.write_html("output/shrinking-events-corr-matrix.html")


