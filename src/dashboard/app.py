from dash import Dash, html, dcc
import mysql.connector
import plotly.express as px
from dynaconf import Dynaconf
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta

from data_loader import load_posts_by_hour


# Build engine using Dynaconf settings
settings = Dynaconf(envvar_prefix="MYSQL", load_dotenv=True)
engine = create_engine(settings.ENGINE_URL, echo=True)
logger = logging.getLogger(__name__)


app = Dash(__name__)


# Test connection to MySQL
end   = datetime.now()
start = end - timedelta(hours=3)


# Load by hour and build a bar chart
df_hour = load_posts_by_hour(start, end)
fig_hour = px.bar(
    df_hour,
    x="hour_of_day",
    y="num_posts",
    title="Posts by Hour of Day (Last 3h)"
)

# 5. Define layout
app.layout = html.Div([
    html.H1("Bluetrends! Dashboard Home"),
    dcc.Graph(figure=fig_hour),
])

# 6. Run server
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8051)
