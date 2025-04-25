# src/dashboard/callbacks.py
from dash import Input, Output, State, callback, dash
import pandas as pd
from components import keyword_sentiment_bars
from data_loader import get_top_keywords_cached
from components import keyword_stats_table

@callback(
    Output("selected-keywords", "data"),
    Input("kw-leaderboard", "clickData"),
    State("selected-keywords", "data"),
    prevent_initial_call=True,
)
def accumulate_clicked_keyword(clickData, stored):
    if not clickData:
        raise dash.exceptions.PreventUpdate

    clicked_kw = clickData["points"][0]["y"]  # bar's y value is keyword
    kw_set = set(stored or [])
    if clicked_kw in kw_set:
        kw_set.remove(clicked_kw)     # toggle off
    else:
        kw_set.add(clicked_kw)
    return list(kw_set)

@callback(
    Output("sentiment-col", "children"),
    Input("selected-keywords", "data"),
)
def update_sentiment_chart(keyword_list):
    return keyword_sentiment_bars(keyword_list)
