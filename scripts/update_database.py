import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_database_schema():
    """Update database schema with missing columns"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="news_omnidb",
            user="crawler",
            password="crawler123",
            port=5432
        )
        cur = conn.cursor()
        
        logger.info("🔄 Updating database schema...")
        
        # Check if sentiment_label column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='articles' AND column_name='sentiment_label'
        """)
        
        if not cur.fetchone():
            logger.info("➕ Adding sentiment_label column to articles table...")
            cur.execute("""
                ALTER TABLE articles 
                ADD COLUMN sentiment_label VARCHAR(20) DEFAULT 'neutral'
            """)
        
        # Check if content_length column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='articles' AND column_name='content_length'
        """)
        
        if not cur.fetchone():
            logger.info("➕ Adding content_length column to articles table...")
            cur.execute("""
                ALTER TABLE articles 
                ADD COLUMN content_length INTEGER DEFAULT 0
            """)
            
            # Update existing rows with content length
            logger.info("📊 Calculating content lengths for existing articles...")
            cur.execute("""
                UPDATE articles 
                SET content_length = LENGTH(content)
                WHERE content IS NOT NULL
            """)
        
        # Check if publish_date column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='articles' AND column_name='publish_date'
        """)
        
        if not cur.fetchone():
            logger.info("➕ Adding publish_date column to articles table...")
            cur.execute("""
                ALTER TABLE articles 
                ADD COLUMN publish_date TIMESTAMP
            """)
        
        # Check if crawler_engine column exists
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='articles' AND column_name='crawler_engine'
        """)
        
        if not cur.fetchone():
            logger.info("➕ Adding crawler_engine column to articles table...")
            cur.execute("""
                ALTER TABLE articles 
                ADD COLUMN crawler_engine VARCHAR(50) DEFAULT 'simple_crawler'
            """)
        
        conn.commit()
        logger.info("✅ Database schema updated successfully!")
        
        # Show current schema
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name='articles'
            ORDER BY ordinal_position
        """)
        
        logger.info("📋 Current articles table schema:")
        for row in cur.fetchall():
            logger.info(f"   {row[0]} ({row[1]}) - nullable: {row[2]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Database update failed: {e}")
        raise

if __name__ == "__main__":
    update_database_schema()