# src/dashboard/pages/keywords.py
from dash import html, dcc
import dash_bootstrap_components as dbc

from components import keyword_leaderboard, keyword_sentiment_bars, render_navbar

layout = dbc.Container(
    [
        html.H1("Keywords"),
        html.P("Explore the most common keywords and their sentiment."),
        html.Hr(),

        dbc.Row(
            dbc.Col(keyword_leaderboard(limit=20), md=12),
            className="mb-4",
        ),


        dbc.Row(
            dbc.Col(keyword_sentiment_bars([]), md=12, id="sentiment-col"),
            className="pt-2",
        ),

        dcc.Store(id="selected-keywords"),
    ],
    fluid=True,
    className="pt-3",
)