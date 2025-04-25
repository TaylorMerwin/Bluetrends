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

CREATE INDEX idx_posts_created_at ON posts(created_at);
CREATE INDEX idx_posts_did        ON posts(did);

CREATE TABLE IF NOT EXISTS keyword_trends (
  keyword               VARCHAR(100) NOT NULL,
  period_start          DATETIME     NOT NULL,
  period_end            DATETIME     NOT NULL,
  post_count            INT          NOT NULL,
  avg_sentiment_score   FLOAT        NOT NULL,
  sentiment_label       VARCHAR(50)  NOT NULL,
  PRIMARY KEY (keyword, period_start, sentiment_label)
) ENGINE=InnoDB;

CREATE INDEX idx_keyword_trends_keyword ON keyword_trends(keyword);