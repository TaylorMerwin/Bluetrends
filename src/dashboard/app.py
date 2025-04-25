# src/dashboard/app.py


import dash_bootstrap_components as dbc
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import pandas as pd
from callbacks import *

from components import render_navbar, render_index_layout
from pages.keywords import layout as keywords_layout
from pages.compare import layout as compare_layout


app = Dash(external_stylesheets=[dbc.themes.CERULEAN], suppress_callback_exceptions=True)

from callbacks_compare import *

index_layout = render_index_layout()

app.layout = html.Div(
    [
        render_navbar(),
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content", children=index_layout),
    ])

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname"),
)
def display_page(pathname):
    if pathname == "/keywords":
        return keywords_layout
    elif pathname == "/compare":
        return compare_layout
    else:
        return index_layout


# ───── Main ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8051, debug=False)
