-- Create NBA Analytics database
CREATE DATABASE nba_analytics;

-- Connect to NBA Analytics database
\c nba_analytics;

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze_nba;
CREATE SCHEMA IF NOT EXISTS silver_nba;
CREATE SCHEMA IF NOT EXISTS gold_nba;

-- Create bronze layer tables
CREATE TABLE IF NOT EXISTS bronze_nba.raw_games (
    game_id VARCHAR(20) PRIMARY KEY,
    game_date DATE NOT NULL,
    home_team_id VARCHAR(3) NOT NULL,
    visitor_team_id VARCHAR(3) NOT NULL,
    home_team_score INTEGER,
    visitor_team_score INTEGER,
    game_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze_nba.raw_social_data (
    tweet_id BIGINT PRIMARY KEY,
    game_hashtag VARCHAR(50),
    text TEXT,
    created_at TIMESTAMP,
    user_followers INTEGER,
    retweet_count INTEGER,
    favorite_count INTEGER,
    sentiment_polarity DECIMAL(3,2),
    sentiment_subjectivity DECIMAL(3,2)
);

CREATE TABLE IF NOT EXISTS bronze_nba.raw_tv_ratings (
    rating_id SERIAL PRIMARY KEY,
    game_date DATE NOT NULL,
    home_team VARCHAR(3) NOT NULL,
    away_team VARCHAR(3) NOT NULL,
    tv_rating DECIMAL(4,2),
    estimated_viewers DECIMAL(5,2),
    is_weekend BOOLEAN,
    is_primetime BOOLEAN,
    day_of_week VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver_nba TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold_nba TO airflow;

-- Create indexes for performance
CREATE INDEX idx_raw_games_date ON bronze_nba.raw_games(game_date);
CREATE INDEX idx_raw_social_date ON bronze_nba.raw_social_data(created_at);
CREATE INDEX idx_raw_ratings_date ON bronze_nba.raw_tv_ratings(game_date);
