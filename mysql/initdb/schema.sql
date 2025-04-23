drop database if exists bluetrends;

CREATE DATABASE IF NOT EXISTS bluetrends;
USE bluetrends;

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    keyword_name VARCHAR(100) UNIQUE NOT NULL
);

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

CREATE TABLE IF NOT EXISTS post_keywords (
  post_id    BIGINT NOT NULL,
  keyword_id BIGINT NOT NULL,
  PRIMARY KEY (post_id, keyword_id),
  CONSTRAINT fk_post_id
    FOREIGN KEY (post_id)
    REFERENCES posts(post_id)
      ON DELETE CASCADE
      ON UPDATE CASCADE,
  CONSTRAINT fk_keyword_id
    FOREIGN KEY (keyword_id)
    REFERENCES keywords(keyword_id)
      ON DELETE CASCADE
      ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS keyword_trends (
  keyword               VARCHAR(100) NOT NULL,
  period_start          DATETIME     NOT NULL,
  period_end            DATETIME     NOT NULL,
  post_count            INT          NOT NULL,
  avg_sentiment_score   FLOAT        NOT NULL,
  PRIMARY KEY (keyword, period_start)
) ENGINE=InnoDB
  COMMENT='Aggregated counts & avg sentiment for each keyword over a fixed window';
