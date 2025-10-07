-- Create articles table
CREATE TABLE IF NOT EXISTS articles (
    article_id VARCHAR(64) PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    content TEXT,
    authors TEXT[],
    domain VARCHAR(255),
    publish_date TIMESTAMP,
    quality_score INTEGER DEFAULT 0,
    category VARCHAR(100),
    crawler_engine VARCHAR(50),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create entities table
CREATE TABLE IF NOT EXISTS article_entities (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR(64) REFERENCES articles(article_id),
    entity_type VARCHAR(50) NOT NULL,
    entity_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain);
CREATE INDEX IF NOT EXISTS idx_articles_publish_date ON articles(publish_date);
CREATE INDEX IF NOT EXISTS idx_articles_quality ON articles(quality_score);
CREATE INDEX IF NOT EXISTS idx_entities_article_id ON article_entities(article_id);
CREATE INDEX IF NOT EXISTS idx_entities_type_name ON article_entities(entity_type, entity_name);