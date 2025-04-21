CREATE DATABASE IF NOT EXISTS bluetrends;
USE bluetrends;


CREATE TABLE IF NOT EXISTS categories (
    category_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS keywords (
    keyword_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    keyword_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_posts (
    post_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    did VARCHAR(255) NOT NULL,
    text TEXT NOT NULL,
    created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
  post_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  raw_post_id     BIGINT UNIQUE,
  did             VARCHAR(255) NOT NULL,
  text            TEXT           NOT NULL,
  created_at      DATETIME       NOT NULL,
  language        VARCHAR(10),
  sfw             BOOLEAN DEFAULT TRUE,
  sentiment_score FLOAT,
  sentiment_label VARCHAR(50),
  category_id     BIGINT,

  CONSTRAINT fk_raw_post_id
    FOREIGN KEY (raw_post_id)
    REFERENCES raw_posts(post_id)
      ON DELETE SET NULL
      ON UPDATE CASCADE,

  CONSTRAINT fk_category_id
    FOREIGN KEY (category_id)
    REFERENCES categories(category_id)
      ON DELETE SET NULL
      ON UPDATE CASCADE
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