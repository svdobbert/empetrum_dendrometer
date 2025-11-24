import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import plotly.express as px

exec(open('src/constants/palettes.py').read())

df = pd.read_csv("input/shrinking-metrics.csv")

def groups_for(v):
    if pd.isna(v):
        return ["missing"]
    groups = []
    if v <= -100: groups.append("<= -100")
    if v <= -50:  groups.append("<= -50")
    if v <= -30:  groups.append("<= -30")
    if v <= -10:  groups.append("<= -10")
    if v < 0:      groups.append("<= 0")
    if v >= 0:     groups.append("> 0")
    return groups

df["shrink_groups"] = df["shrinking_year"].apply(groups_for)

selected_cols = [
   "shrinking_year", "growth_year",
   "n_shrinking_days", "first_shrinking_doy"]

melted = df.explode("shrink_groups").melt(
    id_vars=["shrink_groups"],
    value_vars=selected_cols,
    var_name="metric",
    value_name="value"
).dropna(subset=["value"])

group_order = ["<= -100", "<= -50", "<= -30", "<= -10", "< 0", ">= 0", "missing"]
colors = palette1[:len(group_order)]

fig = px.box(
    melted,
    x="metric",
    y="value",
    color="shrink_groups",
    category_orders={"metric": selected_cols, "shrink_groups": group_order},
    color_discrete_sequence=colors,
    points="outliers",
    labels={"metric": "Metric", "value": "Value [μm/μm/days/doy]", "shrink_groups": "Total shrinking per year [μm]"},
    title="Distribution of Shrinking Metrics by grouped total shrinking per year",
    range_y=[-200, 365],
)
fig.show(renderer="browser")
fig.write_html("output/shrinking-metrics-boxplot.html")

# correlation matrix
input = df[df["shrinking_year"] < -2].copy()
numeric_cols = input.select_dtypes(include=[np.number]).columns.tolist()
numeric_cols = [c for c in numeric_cols if "year" not in c.lower()]
x_vars = numeric_cols
y_vars = ["shrinking_year", "n_shrinking_days", "first_shrinking_doy"]  
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
fig.update_layout(width=300, height=3000)
fig.show(renderer="browser")
fig.write_html("output/shrinking-corr-matrix.html")
