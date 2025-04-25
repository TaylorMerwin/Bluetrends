# Bluetrends

**Real-Time Trend & Sentiment Analysis of Bluesky Posts**  
*A Data Engineering Capstone Project – Dev10 Data Engineering Cohort 2025-3*  

Bluetrends is a full-stack data engineering project that ingests real-time posts from the decentralized social network **Bluesky**, performs sentiment and keyword analysis, and displays insights on a dynamic dashboard.


## Features

- **Real-time ingestion** from the Bluesky Jetstream (WebSocket)
- **Streaming data pipeline** powered by Kafka and PySpark
- **Sentiment analysis** using Hugging Face's `twitter-roberta-base-sentiment`
- **Keyword extraction** using KeyBERT
- **Data orchestration** with Apache Airflow
- **Interactive dashboard** built with Dash (Plotly)
- **MySQL data storage** for post and trend insights

---

## Quickstart

### 1. Clone the Repository

```bash
git clone https://github.com/TaylorMerwin/bluetrends.git
cd bluetrends
```

### 2. Start the Project

Start the project using Docker Compose:

`docker compose up --build -d`

This command will build the necessary Docker images and start all services in detached mode.
* Python app (Websocket client)
* Kafka (Message broker)
* Spark (Streaming processing)
* MySQL (Database)
* Airflow (Orchestration)
* Dash (Dashboard) - available at port 8050

### Access the Dashboard

Once all services are running, open your browser and go to: http://localhost:8051

## References

[Jetstream Overview – Jaz's Blog](https://jazco.dev/2024/09/24/jetstream/)

[Hugging Face: twitter-roberta-base-sentiment](https://huggingface.co/cardiffnlp/twitter-roberta-base-sentiment)

[KeyBERT](https://maartengr.github.io/KeyBERT/)

[AT Protocol Overview](https://atproto.com/guides/overview)

## Acknowledgments

This project was developed as part of the Data Engineering Capstone for the 2025-3 Dev10 cohort.