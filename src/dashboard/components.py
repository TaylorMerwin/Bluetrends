from _plotly_utils.colors import qualitative
from dash import dcc, dash_table, html
import dash_bootstrap_components as dbc
import plotly.express as px
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from dash_bootstrap_components import Card, CardHeader, CardBody, Spinner
from data_loader import load_dashboard_aggregates, get_keyword_sentiment_cached, get_top_keywords_cached, \
    load_keyword_stats

LOOKBACK_HOURS = 72
BUCKET_MINUTES = 15
TIMEZONE_TITLE = "UTC"

SENTIMENT_COLORS = {
    "Positive": "green",
    "Neutral":  "blue",
    "Negative": "red",
}

LANG_CODE_MAP = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "pt": "Portuguese",
    "it": "Italian",
    "ru": "Russian",
    "ar": "Arabic",
    "tr": "Turkish",
    "pl": "Polish",
    "nl": "Dutch",
    "sv": "Swedish",
    "da": "Danish",
    "fi": "Finnish",
    "no": "Norwegian",
    "cs": "Czech",
    "hu": "Hungarian",
    "ro": "Romanian",
    "sk": "Slovak",
    "bg": "Bulgarian",
    "el": "Greek",
    "th": "Thai",
    "vi": "Vietnamese",
    "af": "Afrikaans",
    "so": "Somali",
    "id": "Indonesian",
    "unknown": "Unknown",
}


def hour_to_12h(hour: int) -> str:
    h12 = (hour % 12) or 12
    return f"{h12} {'AM' if hour < 12 else 'PM'}"

def _dashboard_data():
    """
    Return cached aggregates; cache key changes every BUCKET_MINUTES.
    """
    now   = datetime.now(tz=timezone.utc)

    # ── round *down* to nearest 15-minute mark ────────────────────────────
    minute_bucket = (now.minute // BUCKET_MINUTES) * BUCKET_MINUTES
    end   = now.replace(minute=minute_bucket, second=0, microsecond=0)

    start = end - timedelta(hours=LOOKBACK_HOURS)
    return load_dashboard_aggregates(start, end)   # still @lru_cache-decorated


def render_navbar():
    return dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Keywords", href="/keywords")),
            dbc.NavItem(dbc.NavLink("Compare", href="/compare")),
        ],
        brand="Bluetrends social media dashboard",
        brand_href="/",
        color="primary",
        dark=True,
    )

def render_index_layout():
    return dbc.Container(
    [
        render_home(),
    ],
    fluid=True,
    className="pt-3",
)

def render_home():
    return html.Div(
        [
            html.H1("Bluesky network overview"),
            render_fast_facts(),
            html.Hr(),

            html.H2("Traffic"),
            render_total_posts_per_day(),
            html.Hr(),

            html.H2("Sentiment"),
            dbc.Row(
                [
                    dbc.Col(render_sentiment_pie(),      md=4),
                    dbc.Col(render_sentiment_by_hour(),  md=8),
                ],
                className="pt-3",
            ),

            html.Hr(),
            html.H2("Content & Language"),
            dbc.Row(
                [
                    dbc.Col(render_language_bar(full_width=True), md=8),
                    dbc.Col(render_sfw_pie(), md=4),
                ],
                className="pt-3",
            ),
        ]
    )

def render_total_posts_per_day():
    """
    Returns a plotly figure with the total number of posts per day
    """
    end = datetime.now(tz=ZoneInfo("UTC"))
    start = end - timedelta(hours=LOOKBACK_HOURS)
    df_hour = _dashboard_data()["posts_by_hour"]


    fig = px.bar(
        df_hour,
        x="hour_of_day",
        y="num_posts",
        title=f"Posts per Hour (last {LOOKBACK_HOURS} h) — {TIMEZONE_TITLE}",
        labels={"hour_of_day": "Time of Day", "num_posts": "Posts"},
    )
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(24)),
        ticktext=[hour_to_12h(h) for h in range(24)],
        tickangle=-45,
    )

    return dcc.Graph(id="posts-per-hour", figure=fig)

def render_fast_facts():
    facts = _dashboard_data()["fast_facts"]

    def card(title, value):
        return Card(
            [
                CardHeader(title, className="text-center fw-bold"),
                CardBody(html.H2(f"{value:,}", className="display-6 text-center")),
            ],
            className="mx-2",
            style={"minWidth": "12rem"},
        )

    cards_row = dbc.Row(
        [
            dbc.Col(card("Number of posts",     facts["total_posts"]),     md="auto"),
            dbc.Col(card("Unique users",        facts["unique_users"]),    md="auto"),
            dbc.Col(card("Unique keywords",     facts["unique_keywords"]), md="auto"),
        ],
        className="my-4 justify-content-center",
    )

    return Spinner(cards_row, color="primary", type="border", fullscreen=False)

def render_sentiment_pie():
    """
    Pie chart of sentiment share across all posts in the last LOOKBACK_HOURS.
    """
    end   = datetime.now(tz=ZoneInfo("UTC"))
    start = end - timedelta(hours=LOOKBACK_HOURS)
    df = _dashboard_data()["sentiment_totals"]

    fig = px.pie(
        df,
        values="num_posts",
        names="sentiment_label",
        title="Sentiment Share (last 72 h)",
        color="sentiment_label",
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return dcc.Graph(id="sentiment-pie", figure=fig)


def render_sentiment_by_hour():
    """
    Stacked bar of Positive / Neutral / Negative counts per hour.
    """
    end   = datetime.now(tz=ZoneInfo("UTC"))
    start = end - timedelta(hours=LOOKBACK_HOURS)
    df = _dashboard_data()["sentiment_by_hour"]

    fig = px.bar(
        df,
        x="hour_of_day",
        y="num_posts",
        color="sentiment_label",
        title=f"Sentiment per Hour (last {LOOKBACK_HOURS} h)",
        labels={"hour_of_day": "Time of Day", "num_posts": "Posts"},
    )
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(24)),
        ticktext=[hour_to_12h(h) for h in range(24)],
        tickangle=-45,
    )
    return dcc.Graph(id="sentiment-hour", figure=fig)

def render_sfw_pie():
    """
    Pie chart of SFW vs NSFW share in the last LOOKBACK_HOURS window.
    """
    end   = datetime.now(tz=ZoneInfo("UTC"))
    start = end - timedelta(hours=LOOKBACK_HOURS)
    df = _dashboard_data()["sfw_totals"]

    fig = px.pie(
        df,
        values="num_posts",
        names="sfw_flag",
        title="SFW vs NSFW (last 72 h)",
        color="sfw_flag",
        color_discrete_map={"SFW": "green", "NSFW": "red"},
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return dcc.Graph(id="sfw-pie", figure=fig)

def render_language_bar(full_width=False):
    df = _dashboard_data()["language_totals"].head(15)

    # Map code → full name, fallback to code itself
    df["language_name"] = df["language"].map(
        lambda code: LANG_CODE_MAP.get(code, code)
    )

    fig = px.bar(
        df,
        x="num_posts",
        y="language_name",               # use friendly label
        orientation="h",
        title=f"Posts by Language (last {LOOKBACK_HOURS} h)",
        labels={"num_posts": "Posts", "language_name": "Language"},
        color="language_name",
        color_discrete_sequence=px.colors.qualitative.Set3,
    )
    fig.update_layout(
        yaxis=dict(dtick=1),
        margin=dict(l=80 if full_width else 40, r=20, t=50, b=40),
        showlegend=False,
    )
    return dcc.Graph(id="lang-bar", figure=fig, style={"width": "100%"})

def keyword_leaderboard(limit=20):
    """
    Horizontal bar chart of the top `limit` keywords.
    • rainbow colour palette
    • numeric labels on bars
    """
    df = get_top_keywords_cached(limit)

    fig = px.bar(
        df.sort_values("total_posts", ascending=True),
        x="total_posts",
        y="keyword",
        orientation="h",
        title=f"Top {limit} Keywords (last 72 h)",
        labels={"total_posts": "Posts", "keyword": "Keyword"},
        text="total_posts",
        color="keyword",
        color_discrete_sequence=qualitative.Alphabet,
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(
        yaxis=dict(dtick=1),
        showlegend=False,
        margin=dict(l=160, r=20, t=50, b=40),        # wider left margin for labels
    )
    return dcc.Graph(id="kw-leaderboard", figure=fig, style={"width": "100%"})

def keyword_sentiment_bars(keyword_list):
    if not keyword_list:
        return html.Div("Click a keyword to see sentiment breakdown.")

    df = get_keyword_sentiment_cached(keyword_list)

    fig = px.bar(
        df,
        x="keyword",
        y="n",
        color="sentiment_label",
        barmode="group",
        title=f"Sentiment by Keyword (last {LOOKBACK_HOURS} h)",
        labels={"n": "Posts", "keyword": "Keyword"},
        color_discrete_map={
            "Positive": "green",
            "Neutral":  "blue",
            "Negative": "red",
        },
        category_orders={"sentiment_label": ["Positive", "Neutral", "Negative"]},
    )
    return dcc.Graph(id="kw-sentiment-bars", figure=fig, style={"width": "100%"})


def keyword_stats_table(keyword_list):
    if not keyword_list:
        return html.Div("Enter one or more keywords above and press Enter.")

    # 72-hour window, rounded to 15-minute bucket
    now   = datetime.now(tz=timezone.utc)
    minute_bucket = (now.minute // 15) * 15
    end   = now.replace(minute=minute_bucket, second=0, microsecond=0)
    start = end - timedelta(hours=72)

    df = load_keyword_stats(start, end, tuple(keyword_list))
    if df.empty:
        return html.Div("No data for the selected keywords.")

    # ── derive % columns from the three sentiment counts ──────────────────
    df = df.drop(columns=["avg_sentiment_score"])          # remove unwanted col
    sentiment_sum = df[["positive", "neutral", "negative"]].sum(axis=1)

    df["pct_positive"] = (df["positive"] / sentiment_sum * 100).round(1)
    df["pct_neutral"]  = (df["neutral"]  / sentiment_sum * 100).round(1)
    df["pct_negative"] = (df["negative"] / sentiment_sum * 100).round(1)

    # order columns for readability
    df = df[
        [
            "keyword",
            "total_posts",
            "positive", "neutral", "negative",
            "pct_positive", "pct_neutral", "pct_negative",
        ]
    ]

    columns = [
        {"name": "Keyword",      "id": "keyword"},
        {"name": "Total Posts",  "id": "total_posts"},
        {"name": "Positive",     "id": "positive"},
        {"name": "Neutral",      "id": "neutral"},
        {"name": "Negative",     "id": "negative"},
        {"name": "% Positive",   "id": "pct_positive"},
        {"name": "% Neutral",    "id": "pct_neutral"},
        {"name": "% Negative",   "id": "pct_negative"},
    ]

    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=columns,
        style_table={"overflowX": "auto"},
        style_cell={"padding": "6px"},
        sort_action="native",
        fixed_rows={"headers": True},
        style_header={"fontWeight": "bold"},
    )