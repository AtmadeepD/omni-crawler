import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseSchemaUpdater:
    """Update database schema to support enhanced articles"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or "omniparser.db"
        self.conn = None
    
    def connect(self):
        """Connect to database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def get_current_tables(self):
        """Get list of current tables"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    
    def create_enhanced_articles_table(self):
        """Create the enhanced articles table with all 41 fields"""
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS enhanced_articles (
            -- Core Identification
            article_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            canonical_url TEXT,
            
            -- Source Information
            domain TEXT NOT NULL,
            source_type TEXT DEFAULT 'news',
            crawler_engine TEXT,
            crawl_depth INTEGER DEFAULT 0,
            
            -- Content
            content TEXT,
            excerpt TEXT,
            summary TEXT,
            content_length INTEGER DEFAULT 0,
            content_hash TEXT,
            language TEXT DEFAULT 'en',
            
            -- Metadata
            authors TEXT,  -- JSON array as string
            publish_date TEXT,  -- ISO format string
            last_modified TEXT, -- ISO format string
            category TEXT,
            tags TEXT,     -- JSON array as string
            topics TEXT,   -- JSON array as string
            
            -- Content Type
            content_type TEXT DEFAULT 'article',
            schema_type TEXT,
            structured_data TEXT, -- JSON as string
            
            -- Quality Metrics
            quality_score REAL DEFAULT 0.0,
            confidence_score REAL DEFAULT 0.0,
            sentiment TEXT, -- JSON as string
            spam_score REAL DEFAULT 0.0,
            duplicate_of TEXT,
            
            -- Entities & Analysis
            entities TEXT, -- JSON as string
            keywords TEXT, -- JSON array as string
            
            -- Technical Info
            http_status INTEGER DEFAULT 200,
            processing_timestamp TEXT NOT NULL,
            etag TEXT,
            
            -- Engagement Metrics
            read_time INTEGER DEFAULT 0,
            social_shares TEXT, -- JSON as string
            comments_count INTEGER DEFAULT 0,
            outbound_links TEXT, -- JSON array as string
            inbound_links TEXT,  -- JSON array as string
            
            -- Media
            media_attachments TEXT, -- JSON array as string
            amp_url TEXT,
            
            -- Security
            security_score REAL DEFAULT 0.0,
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create indexes for better performance
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_domain ON enhanced_articles(domain);",
            "CREATE INDEX IF NOT EXISTS idx_publish_date ON enhanced_articles(publish_date);",
            "CREATE INDEX IF NOT EXISTS idx_category ON enhanced_articles(category);",
            "CREATE INDEX IF NOT EXISTS idx_quality_score ON enhanced_articles(quality_score);",
            "CREATE INDEX IF NOT EXISTS idx_content_hash ON enhanced_articles(content_hash);",
            "CREATE INDEX IF NOT EXISTS idx_processing_timestamp ON enhanced_articles(processing_timestamp);"
        ]
        
        try:
            cursor = self.conn.cursor()
            
            # Create main table
            cursor.execute(create_table_sql)
            logger.info("✅ Enhanced articles table created/verified")
            
            # Create indexes
            for index_sql in indexes_sql:
                cursor.execute(index_sql)
            logger.info("✅ Database indexes created/verified")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create enhanced articles table: {e}")
            self.conn.rollback()
            return False
    
    def migrate_existing_articles(self):
        """Migrate existing articles to enhanced table (if needed)"""
        try:
            cursor = self.conn.cursor()
            
            # Check if old articles table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='articles'")
            if not cursor.fetchone():
                logger.info("No existing articles table found to migrate")
                return True
            
            # Check if enhanced_articles has data already
            cursor.execute("SELECT COUNT(*) as count FROM enhanced_articles")
            enhanced_count = cursor.fetchone()['count']
            
            if enhanced_count > 0:
                logger.info(f"Enhanced articles table already has {enhanced_count} records, skipping migration")
                return True
            
            # Migrate data from old table
            migrate_sql = """
            INSERT INTO enhanced_articles (
                article_id, title, url, domain, authors, category, quality_score,
                sentiment, content_length, processing_timestamp, publish_date, crawler_engine,
                content, content_hash
            )
            SELECT 
                article_id, title, url, domain, 
                CASE 
                    WHEN authors IS NULL THEN '[]' 
                    WHEN authors = '' THEN '[]'
                    ELSE json_quote(authors) 
                END,
                category, quality_score, 
                CASE 
                    WHEN sentiment IS NULL THEN '{"overall": 0.0}' 
                    ELSE json_quote(sentiment) 
                END,
                content_length, processing_timestamp, publish_date, crawler_engine,
                content,
                -- Generate content hash from existing data
                lower(hex(randomblob(16))) as content_hash
            FROM articles
            """
            
            cursor.execute(migrate_sql)
            migrated_count = cursor.rowcount
            self.conn.commit()
            
            logger.info(f"✅ Migrated {migrated_count} articles to enhanced table")
            return True
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            self.conn.rollback()
            return False
    
    def update_schema(self):
        """Main method to update database schema"""
        try:
            self.connect()
            
            # Create enhanced table
            if not self.create_enhanced_articles_table():
                return False
            
            # Migrate existing data
            if not self.migrate_existing_articles():
                return False
            
            logger.info("🎉 Database schema update completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema update failed: {e}")
            return False
        finally:
            self.close()

def main():
    """Run schema update"""
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    updater = DatabaseSchemaUpdater()
    success = updater.update_schema()
    
    if success:
        print("🎉 Database schema updated successfully!")
        sys.exit(0)
    else:
        print("❌ Database schema update failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()