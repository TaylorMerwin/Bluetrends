# src/dashboard/app.py
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dash_bootstrap_components as dbc
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import pandas as pd

from data_loader import (
    load_posts_by_hour,
    load_sentiment_totals,
    load_sentiment_by_hour,
)

# ───── Config ───────────────────────────────────────────────────────────────
LOOKBACK_HOURS = 72
TIMEZONES = [
    {"label": "UTC",         "value": "UTC"},
    {"label": "US-Pacific",  "value": "US/Pacific"},
    {"label": "US-Mountain", "value": "US/Mountain"},
    {"label": "US-Central",  "value": "US/Central"},
    {"label": "US-Eastern",  "value": "US/Eastern"},
]

# ───── Helper functions ─────────────────────────────────────────────────────
def hour_to_12h(hour: int) -> str:
    h12 = (hour % 12) or 12
    return f"{h12} {'AM' if hour < 12 else 'PM'}"

def local_ticktext(tz_name: str):
    tz = ZoneInfo(tz_name)
    offset = int(datetime.now(tz).utcoffset().total_seconds() // 3600)
    return [hour_to_12h((h + offset) % 24) for h in range(24)]

def fetch_data():
    end   = datetime.utcnow()
    start = end - timedelta(hours=LOOKBACK_HOURS)
    return {
        "posts_by_hour"  : load_posts_by_hour(start, end),
        "sent_totals"    : load_sentiment_totals(start, end),
        "sent_by_hour"   : load_sentiment_by_hour(start, end),
    }

# ───── Chart builders ───────────────────────────────────────────────────────
def build_posts_bar(df, tz_name):
    fig = px.bar(df, x="hour_of_day", y="num_posts",
                 title=f"Posts per Hour — {tz_name}",
                 labels={"hour_of_day": "Time of Day", "num_posts": "Posts"})
    fig.update_xaxes(tickmode="array",
                     tickvals=list(range(24)),
                     ticktext=local_ticktext(tz_name),
                     tickangle=-45)
    return fig

def build_sentiment_pie(df):
    fig = px.pie(df,
                 values="num_posts",
                 names="sentiment_label",
                 title="Sentiment Share (all posts)")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig

def build_sentiment_hour(df, tz_name):
    fig = px.bar(df,
                 x="hour_of_day",
                 y="num_posts",
                 color="sentiment_label",
                 title=f"Sentiment per Hour — {tz_name}",
                 labels={"hour_of_day": "Time of Day", "num_posts": "Posts"})
    fig.update_xaxes(tickmode="array",
                     tickvals=list(range(24)),
                     ticktext=local_ticktext(tz_name),
                     tickangle=-45)
    return fig

# ───── Dash layout & callbacks ──────────────────────────────────────────────
app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])

app.layout = dbc.Container(
    [
        html.H2("Bluetrends Dashboard"),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        TIMEZONES,
                        value="UTC",
                        id="tz-select",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                    width="auto",
                ),
            ],
            className="mb-2",
        ),
        dcc.Tabs(
            id="tabs",
            value="overview",
            children=[
                dcc.Tab(label="Overview", value="overview"),
                dcc.Tab(label="Activity", value="activity"),
            ],
        ),
        html.Div(id="tab-content"),
    ],
    fluid=True,
    className="pt-3",
)

@app.callback(
    Output("tab-content", "children"),
    Input("tabs", "value"),
    Input("tz-select", "value"),
)
def render_tab(tab_id, tz_name):
    data = fetch_data()

    if tab_id == "overview":
        # --- build 3 figures
        bar_fig  = build_posts_bar(data["posts_by_hour"], tz_name)
        pie_fig  = build_sentiment_pie(data["sent_totals"])
        hour_fig = build_sentiment_hour(data["sent_by_hour"], tz_name)

        return dbc.Container(
            [
                dbc.Row([dbc.Col(dcc.Graph(figure=bar_fig), width=12)]),
                html.Hr(),
                html.H4("Sentiment"),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(figure=pie_fig),  md=4),
                        dbc.Col(dcc.Graph(figure=hour_fig), md=8),
                    ]
                ),
            ],
            fluid=True,
        )

    # ─── Activity tab (unchanged – reuse bar as a line) ────────────────────
    line_fig = build_posts_bar(data["posts_by_hour"], tz_name)
    line_fig.update_traces(mode="lines+markers")
    line_fig.update_layout(title=f"Posting Activity by Hour — {tz_name}")
    return dbc.Row([dbc.Col(dcc.Graph(figure=line_fig), width=12)])

# ───── Main ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=True)
