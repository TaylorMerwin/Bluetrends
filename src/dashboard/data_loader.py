# src/dashboard/data_loader.py
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from dynaconf import Dynaconf
from typing import Union, Tuple
from datetime import datetime, timedelta
from functools import lru_cache
import time


settings = Dynaconf(envvar_prefix="MYSQL", load_dotenv=True)
engine = create_engine(settings.ENGINE_URL, echo=True)
logger = logging.getLogger(__name__)

ENGINE_URL = settings.ENGINE_URL
if not ENGINE_URL:
    raise RuntimeError("Please set DB_ENGINE_URL in your environment")
engine = create_engine(ENGINE_URL, echo=False, pool_pre_ping=True)

_df_posts_by_hour_cache: dict[Tuple[str, str], pd.DataFrame] = {}

def load_posts_by_hour(
        start: Union[str, datetime],
        end: Union[str, datetime]
) -> pd.DataFrame:
    """
    Load posts by hour from MySQL database.
    """

    key = (str(start), str(end))
    if key in _df_posts_by_hour_cache:
        return _df_posts_by_hour_cache[key]

    query = """
        SELECT
            HOUR(created_at) AS hour_of_day,
            COUNT(*)        AS num_posts
        FROM posts
        WHERE created_at >= %s
          AND created_at <  %s
        GROUP BY hour_of_day
        ORDER BY hour_of_day
    """
    df = pd.read_sql(query, engine, params=(start, end))
    _df_posts_by_hour_cache[key] = df
    logger.info(f"Loaded {len(df)} rows for posts by hour from {start} to {end}")
    return df


def _rounded_minute(now=None, bucket=15):
    now = now or datetime.utcnow()
    minute_bucket = (now.minute // bucket) * bucket
    return now.replace(minute=minute_bucket, second=0, microsecond=0)


def load_sentiment_totals(start, end):
    """
    Returns DataFrame with columns:
        sentiment_label | num_posts
    """
    sql = """
        SELECT sentiment_label, COUNT(*) AS num_posts
        FROM posts
        WHERE created_at BETWEEN %s AND %s
        GROUP BY sentiment_label
    """
    return pd.read_sql(sql, engine, params=(start, end))


def load_sentiment_by_hour(start, end):
    """
    Returns DataFrame with columns:
        hour_of_day | sentiment_label | num_posts
    """
    sql = """
        SELECT
            HOUR(created_at)      AS hour_of_day,
            sentiment_label,
            COUNT(*)              AS num_posts
        FROM posts
        WHERE created_at BETWEEN %s AND %s
        GROUP BY hour_of_day, sentiment_label
        ORDER BY hour_of_day
    """
    return pd.read_sql(sql, engine, params=(start, end))


def load_fast_facts():
    sql = """
    SELECT
      (SELECT COUNT(*) FROM posts)                          AS total_posts,
      (SELECT COUNT(DISTINCT did) FROM posts)               AS unique_users,
      (SELECT COUNT(DISTINCT keyword) FROM keyword_trends)  AS unique_keywords
    """
    row = pd.read_sql(sql, engine).iloc[0]
    return row.to_dict()

def load_sfw_totals(start, end):
    """
    Returns a DataFrame with columns:
        sfw_flag | num_posts
    where sfw_flag is 'SFW' or 'NSFW'.
    """
    sql = """
        SELECT
            CASE sfw WHEN 1 THEN 'SFW' ELSE 'NSFW' END AS sfw_flag,
            COUNT(*) AS num_posts
        FROM posts
        WHERE created_at BETWEEN %s AND %s
        GROUP BY sfw_flag
    """
    return pd.read_sql(sql, engine, params=(start, end))


def _dashboard_sql():
    return """
    SELECT
        'posts_by_hour'        AS metric,
        HOUR(created_at)       AS col1,
        NULL                   AS col2,
        COUNT(*)               AS value
    FROM posts
    WHERE created_at BETWEEN %(start)s AND %(end)s
    GROUP BY HOUR(created_at)

    UNION ALL

    SELECT
        'sentiment_totals',
        NULL,
        sentiment_label,
        COUNT(*)
    FROM posts
    WHERE created_at BETWEEN %(start)s AND %(end)s
    GROUP BY sentiment_label

    UNION ALL

    SELECT
        'sentiment_by_hour',
        HOUR(created_at),
        sentiment_label,
        COUNT(*)
    FROM posts
    WHERE created_at BETWEEN %(start)s AND %(end)s
    GROUP BY HOUR(created_at), sentiment_label

    UNION ALL

    SELECT
        'sfw_totals',
        NULL,
        CASE sfw WHEN 1 THEN 'SFW' ELSE 'NSFW' END,
        COUNT(*)
    FROM posts
    WHERE created_at BETWEEN %(start)s AND %(end)s
    GROUP BY CASE sfw WHEN 1 THEN 'SFW' ELSE 'NSFW' END
        
    UNION ALL

    SELECT
        'language_totals'           AS metric,
        NULL                        AS col1,
        COALESCE(language, 'unknown') AS col2,
        COUNT(*)                    AS value
    FROM posts
    WHERE created_at BETWEEN %(start)s AND %(end)s
    GROUP BY col2

    UNION ALL

    SELECT 'fast_facts', NULL, 'TOTAL_POSTS',     COUNT(*)            FROM posts
    UNION ALL
    SELECT 'fast_facts', NULL, 'UNIQUE_USERS',    COUNT(DISTINCT did) FROM posts
    UNION ALL
    SELECT 'fast_facts', NULL, 'UNIQUE_KEYWORDS', COUNT(DISTINCT keyword)
      FROM keyword_trends
    ;
    """

@lru_cache(maxsize=8)   # cache keyed by (start, end) tuple
def load_dashboard_aggregates(start: datetime, end: datetime) -> dict:
    """
    Run ONE aggregate query and slice the result into DataFrames / dicts
    needed by the home page. Result is cached.
    """
    raw = pd.read_sql(
        _dashboard_sql(),
        engine,
        params=dict(start=start, end=end)
    )

    # helper to pull out one metric slice
    def slice_df(metric: str) -> pd.DataFrame:
        return raw.loc[raw.metric == metric].copy()

    ff_raw = (
        slice_df("fast_facts")
        .set_index("col2")["value"]
        .to_dict()
    )
    fast_facts = {
        "total_posts":     ff_raw.get("TOTAL_POSTS", 0),
        "unique_users":    ff_raw.get("UNIQUE_USERS", 0),
        "unique_keywords": ff_raw.get("UNIQUE_KEYWORDS", 0),
    }

    return {
        "posts_by_hour": (
            slice_df("posts_by_hour")
            .rename(columns={"col1": "hour_of_day", "value": "num_posts"})
        ),
        "sentiment_totals": (
            slice_df("sentiment_totals")
            .rename(columns={"col2": "sentiment_label", "value": "num_posts"})
        ),
        "sentiment_by_hour": (
            slice_df("sentiment_by_hour")
            .rename(columns={"col1": "hour_of_day",
                             "col2": "sentiment_label",
                             "value": "num_posts"})
        ),
        "sfw_totals": (
            slice_df("sfw_totals")
            .rename(columns={"col2": "sfw_flag", "value": "num_posts"})
        ),
        "language_totals": (
            slice_df("language_totals")
            .rename(columns={"col2": "language", "value": "num_posts"})
            .sort_values("num_posts", ascending=False)
        ),
        "fast_facts": fast_facts,      # ← normalized dict
    }

@lru_cache(maxsize=32)
def load_top_keywords(start, end, limit=20):
    sql = """
        SELECT keyword,
               SUM(post_count) AS total_posts
        FROM keyword_trends
        WHERE period_start BETWEEN %s AND %s
        GROUP BY keyword
        ORDER BY total_posts DESC
        LIMIT %s;
    """
    return pd.read_sql(sql, engine, params=(start, end, limit))

@lru_cache(maxsize=128)
def load_keyword_sentiment(start, end, keyword_list):
    """
    Returns sentiment counts for the selected keywords.
    """
    placeholder = ",".join(["%s"] * len(keyword_list))  # (%s,%s,%s)
    sql = f"""
        SELECT keyword,
               sentiment_label,
               SUM(post_count) AS n,
               ROUND(SUM(avg_sentiment_score * post_count) /
                     SUM(post_count), 3) AS avg_score
        FROM keyword_trends
        WHERE period_start BETWEEN %s AND %s
          AND keyword IN ({placeholder})
        GROUP BY keyword, sentiment_label;
    """
    params = (start, end, *keyword_list)
    return pd.read_sql(sql, engine, params=params)

def get_top_keywords_cached(limit=20):
    now_rounded = _rounded_minute()
    start = now_rounded - timedelta(hours=72)
    return load_top_keywords(start, now_rounded, limit)

def get_keyword_sentiment_cached(keyword_list):
    now_rounded = _rounded_minute()
    start = now_rounded - timedelta(hours=72)
    return load_keyword_sentiment(start, now_rounded, tuple(keyword_list))

@lru_cache(maxsize=64)
def load_keyword_stats(start, end, keyword_tuple):
    """
    Return one row per keyword with totals & sentiment counts.
    """
    placeholder = ",".join(["%s"] * len(keyword_tuple))
    sql = f"""
        SELECT keyword,
               SUM(post_count)                                             AS total_posts,
               ROUND(SUM(avg_sentiment_score * post_count) /
                     SUM(post_count), 3)                                   AS avg_sentiment_score,
               SUM(CASE WHEN sentiment_label='Positive' THEN post_count ELSE 0 END) AS positive,
               SUM(CASE WHEN sentiment_label='Neutral'  THEN post_count ELSE 0 END) AS neutral,
               SUM(CASE WHEN sentiment_label='Negative' THEN post_count ELSE 0 END) AS negative
        FROM keyword_trends
        WHERE period_start BETWEEN %s AND %s
          AND keyword IN ({placeholder})
        GROUP BY keyword;
    """
    params = (start, end, *keyword_tuple)
    return pd.read_sql(sql, engine, params=params)