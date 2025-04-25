# src/dashboard/pages/compare.py
from dash import html, dcc
import dash_bootstrap_components as dbc
from components import render_navbar, keyword_stats_table, get_top_keywords_cached

# options for dropdown (top 200 keywords for autocomplete convenience)
options = [{"label": kw, "value": kw}
           for kw in get_top_keywords_cached(200)["keyword"].tolist()]

layout = dbc.Container(
    [
        html.H1("Compare Keywords"),
        html.P("Select or type keywords to compare usage and sentiment."),
        dcc.Dropdown(
            id="compare-keywords",
            options=options,
            multi=True,
            placeholder="Type keywords…",
            style={"marginBottom": "1rem"},
        ),
        html.Div(id="compare-table"),
    ],
    fluid=True,
    className="pt-3",
)