
# src/dashboard/callbacks_compare.py
from dash import Input, Output, callback
from components import keyword_stats_table

@callback(
    Output("compare-table", "children"),
    Input("compare-keywords", "value"),
)
def update_compare_table(keyword_list):
    return keyword_stats_table(keyword_list or [])