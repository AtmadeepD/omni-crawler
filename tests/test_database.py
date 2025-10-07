import psycopg2

def test_database():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="news_omnidb",
            user="crawler",
            password="crawler123",
            port=5432
        )
        cur = conn.cursor()
        
        # Check tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        tables = [row[0] for row in cur.fetchall()]
        print("📊 Tables in database:", tables)
        
        # Check articles table structure
        if 'articles' in tables:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name='articles'
            """)
            columns = {row[0]: row[1] for row in cur.fetchall()}
            print("📋 Articles table columns:", columns)
            
            # Count articles
            cur.execute("SELECT COUNT(*) FROM articles")
            count = cur.fetchone()[0]
            print(f"📄 Total articles: {count}")
            
            # Show sample articles
            cur.execute("SELECT title, domain, quality_score FROM articles LIMIT 3")
            print("🔍 Sample articles:")
            for row in cur.fetchall():
                print(f"  - {row[0]} ({row[1]}) - Quality: {row[2]}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")

if __name__ == "__main__":
    test_database()