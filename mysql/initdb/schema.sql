CREATE DATABASE IF NOT EXISTS bluetrends;
USE bluetrends;


CREATE TABLE IF NOT EXISTS posts (
  post_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  post_uuid       CHAR(36)       NOT NULL UNIQUE,
  did             VARCHAR(255)   NOT NULL,
  text            TEXT           NOT NULL,
  created_at      DATETIME       NOT NULL,
  language        VARCHAR(10),
  sfw             BOOLEAN DEFAULT TRUE,
  sentiment_score FLOAT,
  sentiment_label VARCHAR(50),
  keywords JSON NULL
);

CREATE TABLE IF NOT EXISTS keyword_trends (
  keyword               VARCHAR(100) NOT NULL,
  period_start          DATETIME     NOT NULL,
  period_end            DATETIME     NOT NULL,
  post_count            INT          NOT NULL,
  avg_sentiment_score   FLOAT        NOT NULL,
  sentiment_label       VARCHAR(50)  NOT NULL,
  PRIMARY KEY (keyword, period_start, sentiment_label)
) ENGINE=InnoDB;
