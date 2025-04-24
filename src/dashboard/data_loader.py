# src/dashboard/data_loader.py
import pandas as pd
from sqlalchemy import create_engine
import logging
from dynaconf import Dynaconf
from typing import Union, Tuple
from datetime import datetime

settings = Dynaconf(envvar_prefix="MYSQL", load_dotenv=True)
engine = create_engine(settings.ENGINE_URL, echo=True)
logger = logging.getLogger(__name__)

ENGINE_URL = settings.ENGINE_URL
if not ENGINE_URL:
    raise RuntimeError("Please set DB_ENGINE_URL in your environment")
engine = create_engine(ENGINE_URL, echo=False, pool_pre_ping=True)

# Caches
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