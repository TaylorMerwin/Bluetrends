# src/dashboard/tabs/overview.py
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
from ..data_loader import load_posts_by_hour

def layout(start, end):
    """Return a Div containing all elements for the Overview tab."""
    df_hour = load_posts_by_hour(start, end)

    fig_hour = px.bar(
        df_hour,
        x="hour_of_day",
        y="num_posts",
        title="Posts by Hour of Day",
        labels={"hour_of_day": "Hour (0-23)", "num_posts": "Number of posts"},
    )

    # You can add KPI cards here later
    return dbc.Row(
        [
            dbc.Col(dcc.Graph(figure=fig_hour), width=12)
        ]
    )