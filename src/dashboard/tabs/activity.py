# src/dashboard/tabs/activity.py
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
from ..data_loader import load_posts_by_hour

def layout(start, end):
    """
    More detailed traffic patterns.
    For now we reuse the same 'posts per hour' chart, but you’ll likely add:
      • day-of-week × hour heat-map
      • posting trend line
    """
    df_hour = load_posts_by_hour(start, end)
    fig_hour = px.line(
        df_hour,
        x="hour_of_day",
        y="num_posts",
        markers=True,
        title="Posting Activity by Hour (selected range)",
        labels={"hour_of_day": "Hour (0-23)", "num_posts": "Posts"},
    )

    return dbc.Row(
        [
            dbc.Col(dcc.Graph(figure=fig_hour), width=12)
        ]
    )
