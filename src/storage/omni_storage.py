from elasticsearch import Elasticsearch
import psycopg2
import redis
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OmniStorageManager:
    def __init__(self, es_host='localhost', pg_host='localhost', redis_host='localhost'):
        try:
            # Elasticsearch for search and analytics
            self.es = Elasticsearch([f'http://{es_host}:9200'])
            if self.es.ping():
                logger.info("✅ Elasticsearch connection successful")
            else:
                logger.error("❌ Elasticsearch connection failed")
            
            # PostgreSQL for metadata and relationships
            self.pg_conn = psycopg2.connect(
                host=pg_host,
                database="news_omnidb",
                user="crawler",
                password="crawler123",
                port=5432
            )
            logger.info("✅ PostgreSQL connection successful")
            
            # Redis for caching and queues
            self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
            self.redis.ping()
            logger.info("✅ Redis connection successful")
            
        except Exception as e:
            logger.error(f"❌ Storage initialization failed: {e}")
            raise
    
    def store_article(self, enhanced_article):
        """Store across multiple storage systems"""
        logger.info(f"💾 Storing article: {enhanced_article['article_id']}")
        
        try:
            # 1. Elasticsearch for search
            es_success = self._store_in_elasticsearch(enhanced_article)
            
            # 2. PostgreSQL for relational data
            pg_success = self._store_in_postgresql(enhanced_article)
            
            # 3. Redis for recent articles cache
            redis_success = self._cache_in_redis(enhanced_article)
            
            if es_success and pg_success:
                logger.info(f"✅ Article stored successfully: {enhanced_article['article_id']}")
                return True
            else:
                logger.error(f"❌ Article storage failed: {enhanced_article['article_id']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Storage error: {e}")
            return False
    
    def _store_in_elasticsearch(self, article):
        """Store in Elasticsearch for full-text search"""
        try:
            es_doc = self._prepare_es_document(article)
            
            # Create index if it doesn't exist
            if not self.es.indices.exists(index='news-articles'):
                self.es.indices.create(
                    index='news-articles',
                    body={
                        'settings': {
                            'number_of_shards': 1,
                            'number_of_replicas': 0
                        },
                        'mappings': {
                            'properties': {
                                'title': {'type': 'text'},
                                'content': {'type': 'text'},
                                'authors': {'type': 'keyword'},
                                'domain': {'type': 'keyword'},
                                'category': {'type': 'keyword'},
                                'sentiment': {
                                    'properties': {
                                        'polarity': {'type': 'float'},
                                        'label': {'type': 'keyword'}
                                    }
                                },
                                'quality_score': {'type': 'integer'},
                                'processing_timestamp': {'type': 'date'}
                            }
                        }
                    }
                )
                logger.info("✅ Created Elasticsearch index 'news-articles'")
            
            # Index the document
            response = self.es.index(
                index='news-articles',
                id=article['article_id'],
                body=es_doc
            )
            
            logger.info(f"✅ Elasticsearch: Stored {article['article_id']}")
            return response['result'] in ['created', 'updated']
            
        except Exception as e:
            logger.error(f"❌ Elasticsearch storage error: {e}")
            return False
    
    def _store_in_postgresql(self, article):
        """Store in PostgreSQL database with all columns"""
        try:
            with self.pg_conn.cursor() as cur:
                # Check if articles table exists, create if not
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'articles'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    self._create_tables(cur)
                    logger.info("✅ Created database tables")
                
                # Insert main article with all available fields
                cur.execute("""
                    INSERT INTO articles 
                    (article_id, url, title, content, authors, domain, publish_date, 
                     quality_score, category, crawler_engine, processing_timestamp,
                     sentiment_label, content_length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_id) DO UPDATE SET
                    title = EXCLUDED.title, 
                    content = EXCLUDED.content,
                    quality_score = EXCLUDED.quality_score,
                    category = EXCLUDED.category,
                    sentiment_label = EXCLUDED.sentiment_label,
                    content_length = EXCLUDED.content_length,
                    processing_timestamp = EXCLUDED.processing_timestamp
                """, (
                    article['article_id'], 
                    article['url'], 
                    article['title'],
                    article.get('content', '')[:10000],  # Limit content length
                    article.get('authors', []),
                    article.get('domain', ''),
                    article.get('publish_date'),
                    article.get('quality_score', 0),
                    article.get('category', 'general'),
                    article.get('crawler_engine', 'unknown'),
                    article.get('processing_timestamp'),
                    article.get('sentiment_label', 'neutral'),
                    article.get('content_length', 0)
                ))
                
                # Store entities
                entities = article.get('entities', {})
                for entity_type in ['persons', 'organizations', 'locations']:
                    for entity_name in entities.get(entity_type, []):
                        try:
                            cur.execute(
                                "INSERT INTO article_entities (article_id, entity_type, entity_name) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                                (article['article_id'], entity_type[:-1], entity_name)  # Remove 's' from plural
                            )
                        except Exception as e:
                            logger.warning(f"Entity insertion warning: {e}")
                            continue
                
                self.pg_conn.commit()
                logger.info(f"✅ PostgreSQL: Stored {article['article_id']}")
                return True
                
        except Exception as e:
            logger.error(f"❌ PostgreSQL storage error: {e}")
            self.pg_conn.rollback()
            return False
    
    def _create_tables(self, cur):
        """Create necessary database tables"""
        # Create articles table
        cur.execute("""
            CREATE TABLE articles (
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
        """)
        
        # Create entities table
        cur.execute("""
            CREATE TABLE article_entities (
                id SERIAL PRIMARY KEY,
                article_id VARCHAR(64) REFERENCES articles(article_id),
                entity_type VARCHAR(50) NOT NULL,
                entity_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(article_id, entity_type, entity_name)
            );
        """)
        
        # Create indexes for performance
        cur.execute("CREATE INDEX idx_articles_domain ON articles(domain);")
        cur.execute("CREATE INDEX idx_articles_publish_date ON articles(publish_date);")
        cur.execute("CREATE INDEX idx_articles_quality ON articles(quality_score);")
        cur.execute("CREATE INDEX idx_entities_article_id ON article_entities(article_id);")
        cur.execute("CREATE INDEX idx_entities_type_name ON article_entities(entity_type, entity_name);")
    
    def _cache_in_redis(self, article):
        """Cache article in Redis for quick access"""
        try:
            # Cache article metadata for 1 hour
            cache_key = f"article:{article['article_id']}"
            self.redis.setex(
                cache_key, 
                3600,  # 1 hour TTL
                json.dumps({
                    'title': article['title'],
                    'url': article['url'],
                    'domain': article.get('domain', ''),
                    'quality_score': article.get('quality_score', 0),
                    'category': article.get('category', 'general'),
                    'processing_timestamp': article.get('processing_timestamp')
                })
            )
            
            # Add to recent articles list
            self.redis.lpush('recent_articles', article['article_id'])
            self.redis.ltrim('recent_articles', 0, 99)  # Keep only 100 most recent
            
            logger.info(f"✅ Redis: Cached {article['article_id']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Redis caching error: {e}")
            return False
    
    def _prepare_es_document(self, article):
        """Format for Elasticsearch"""
        return {
            'title': article['title'],
            'content': article.get('content', ''),
            'authors': article.get('authors', []),
            'publish_date': article.get('publish_date'),
            'domain': article.get('domain', ''),
            'entities': article.get('entities', {}),
            'sentiment': {
                'polarity': article.get('sentiment_polarity', 0),
                'label': article.get('sentiment_label', 'neutral')
            },
            'quality_score': article.get('quality_score', 0),
            'category': article.get('category', 'general'),
            'key_phrases': article.get('key_phrases', []),
            'processing_timestamp': article.get('processing_timestamp'),
            'crawler_engine': article.get('crawler_engine', 'unknown'),
            'content_length': len(article.get('content', '')),
            'word_count': len(article.get('content', '').split()),
            'discovery_source': article.get('discovery_source', 'unknown')
        }
    
    def get_article_count(self):
        """Get total article count from PostgreSQL"""
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM articles;")
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting article count: {e}")
            return 0
    
    def get_recent_articles(self, limit=10):
        """Get recent articles from Redis cache"""
        try:
            recent_ids = self.redis.lrange('recent_articles', 0, limit-1)
            articles = []
            for article_id in recent_ids:
                cached = self.redis.get(f"article:{article_id}")
                if cached:
                    articles.append(json.loads(cached))
            return articles
        except Exception as e:
            logger.error(f"Error getting recent articles: {e}")
            return []
    
    def search_articles(self, query, size=10):
        """Search articles in Elasticsearch"""
        try:
            response = self.es.search(
                index='news-articles',
                body={
                    'query': {
                        'multi_match': {
                            'query': query,
                            'fields': ['title^2', 'content', 'key_phrases']
                        }
                    },
                    'size': size,
                    'sort': [
                        {'quality_score': {'order': 'desc'}},
                        {'processing_timestamp': {'order': 'desc'}}
                    ]
                }
            )
            return [hit['_source'] for hit in response['hits']['hits']]
        except Exception as e:
            logger.error(f"Error searching articles: {e}")
            return []
    
    def close(self):
        """Clean up connections"""
        try:
            if hasattr(self, 'pg_conn'):
                self.pg_conn.close()
                logger.info("🔌 PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Error closing PostgreSQL: {e}")
        
        logger.info("🔌 Storage connections closed")

# Test function
def test_storage():
    """Test the storage system"""
    print("🧪 Testing Storage System...")
    
    try:
        storage = OmniStorageManager()
        
        # Sample article data
        sample_article = {
            'article_id': 'article_test_123',
            'title': 'Test Article Title',
            'content': 'This is a test article content for storage testing. It contains some sample text to verify that the storage system is working correctly across all three storage layers: Elasticsearch, PostgreSQL, and Redis.',
            'url': 'https://example.com/test-article',
            'authors': ['Test Author'],
            'domain': 'example.com',
            'publish_date': '2024-01-07T12:00:00Z',
            'quality_score': 85,
            'category': 'technology',
            'crawler_engine': 'simple_crawler',
            'processing_timestamp': datetime.utcnow().isoformat(),
            'entities': {
                'persons': ['Test Person'],
                'organizations': ['Test Org'],
                'locations': ['Test City']
            },
            'sentiment_polarity': 0.1,
            'sentiment_label': 'neutral',
            'key_phrases': ['test article', 'storage testing', 'sample content'],
            'discovery_source': 'rss'
        }
        
        # Test storage
        success = storage.store_article(sample_article)
        print(f"✅ Storage test: {'PASSED' if success else 'FAILED'}")
        
        # Test article count
        count = storage.get_article_count()
        print(f"📊 Total articles in database: {count}")
        
        # Test recent articles
        recent = storage.get_recent_articles(5)
        print(f"🕒 Recent articles cached: {len(recent)}")
        
        # Test search
        results = storage.search_articles('test', 3)
        print(f"🔍 Search results: {len(results)} articles found")
        
        storage.close()
        
    except Exception as e:
        print(f"❌ Storage test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_storage()