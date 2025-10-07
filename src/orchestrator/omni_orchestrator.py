import asyncio
import yaml
import logging
from datetime import datetime
import time
import sys
import os

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from discovery.url_discovery_engine import OmniURLDiscoverer
from crawler.simple_crawler import SimpleCrawler
from processing.content_enhancement import ContentEnhancementPipeline
from storage.omni_storage import OmniStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('omni_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OmniOrchestrator:
    def __init__(self, config_path='config/master_config.yaml'):
        self.config = self._load_config(config_path)
        self.stats = {
            'urls_discovered': 0,
            'articles_crawled': 0,
            'articles_processed': 0,
            'articles_stored': 0,
            'errors': 0,
            'start_time': datetime.utcnow()
        }
        
        # Initialize components
        logger.info("🚀 Initializing Omni-Crawler Components...")
        self.discoverer = OmniURLDiscoverer(
            redis_host=self.config.get('redis_host', 'localhost')
        )
        self.processor = ContentEnhancementPipeline()
        self.storage = OmniStorage(
            es_host=self.config.get('es_host', 'localhost'),
            pg_host=self.config.get('pg_host', 'localhost'), 
            redis_host=self.config.get('redis_host', 'localhost')
        )
        logger.info("✅ All components initialized successfully")
    
    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"❌ Config load error: {e}")
            # Return default config
            return {
                'crawler': {
                    'max_concurrent_crawls': 3,
                    'crawl_interval_seconds': 300
                },
                'discovery': {
                    'rss_feeds': [
                        "https://feeds.bbci.co.uk/news/rss.xml",
                        "https://feeds.npr.org/1001/rss.xml"
                    ]
                }
            }
    
    async def run_single_cycle(self):
        """Run one complete crawl cycle"""
        cycle_start = datetime.utcnow()
        logger.info(f"🔄 Starting crawl cycle at {cycle_start}")
        
        try:
            # Step 1: Discover URLs
            logger.info("🔍 Phase 1: URL Discovery")
            urls = await self.discoverer.run_discovery()
            self.stats['urls_discovered'] += len(urls)
            logger.info(f"📰 Discovered {len(urls)} new URLs")
            
            if not urls:
                logger.warning("⚠️ No URLs discovered in this cycle")
                return
            
            # Step 2: Crawl Articles
            logger.info("🕷️ Phase 2: Article Crawling")
            crawled_articles = await self._crawl_articles(urls)
            self.stats['articles_crawled'] += len(crawled_articles)
            logger.info(f"📄 Successfully crawled {len(crawled_articles)} articles")
            
            if not crawled_articles:
                logger.warning("⚠️ No articles successfully crawled")
                return
            
            # Step 3: Process Articles
            logger.info("🔧 Phase 3: Content Processing")
            processed_articles = await self._process_articles(crawled_articles)
            self.stats['articles_processed'] += len(processed_articles)
            logger.info(f"🎯 Processed {len(processed_articles)} articles")
            
            # Step 4: Store Articles
            logger.info("💾 Phase 4: Storage")
            stored_count = await self._store_articles(processed_articles)
            self.stats['articles_stored'] += stored_count
            logger.info(f"💽 Stored {stored_count} articles")
            
            # Step 5: Report
            cycle_time = (datetime.utcnow() - cycle_start).total_seconds()
            self._report_cycle_stats(cycle_time, len(urls), len(crawled_articles), stored_count)
            
        except Exception as e:
            logger.error(f"❌ Cycle error: {e}")
            self.stats['errors'] += 1
    
    async def _crawl_articles(self, urls):
        """Crawl multiple articles with concurrency control"""
        crawled_articles = []
        max_concurrent = self.config.get('crawler', {}).get('max_concurrent_crawls', 3)
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def crawl_with_limit(url_info):
            async with semaphore:
                async with SimpleCrawler() as crawler:
                    result = await crawler.crawl_url(url_info['url'], url_info)
                    if result:
                        # Add discovery metadata
                        result['discovery_source'] = url_info.get('source', 'unknown')
                        result['discovery_timestamp'] = url_info.get('discovery_timestamp')
                        return result
                    return None
        
        # Create tasks for all URLs
        tasks = [crawl_with_limit(url_info) for url_info in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful results
        for result in results:
            if result and not isinstance(result, Exception):
                crawled_articles.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Crawl task error: {result}")
        
        return crawled_articles
    
    async def _process_articles(self, raw_articles):
        """Process articles with NLP enhancement"""
        processed_articles = []
        
        for article in raw_articles:
            try:
                processed = self.processor.process_article(article)
                processed_articles.append(processed)
            except Exception as e:
                logger.error(f"Processing error for {article.get('url', 'unknown')}: {e}")
                self.stats['errors'] += 1
        
        return processed_articles
    
    async def _store_articles(self, processed_articles):
        """Store processed articles"""
        stored_count = 0

        for article in processed_articles:
            try:
                # Use enhanced save method
                result = self.storage.save_enhanced_article(article)
                if result['success']:
                    stored_count += 1
                    print(f"✅ Enhanced article saved: {result['article_id']}")
                    print(f"   Quality: {result['quality_score']:.2f}, Confidence: {result['confidence_score']:.2f}")
                else:
                    logger.error(f"Storage failed for {article.get('article_id', 'unknown')}: {result.get('error', 'Unknown error')}")
                    self.stats['errors'] += 1
            except Exception as e:
                logger.error(f"Storage error for {article.get('article_id', 'unknown')}: {e}")
                self.stats['errors'] += 1

        return stored_count
    
    def _report_cycle_stats(self, cycle_time, urls_found, articles_crawled, articles_stored):
        """Report cycle statistics"""
        logger.info(f"""
📊 CYCLE COMPLETE 📊
⏱️  Cycle Time: {cycle_time:.2f}s
🔍 URLs Found: {urls_found}
🕷️ Articles Crawled: {articles_crawled}
💾 Articles Stored: {articles_stored}
✅ Success Rate: {(articles_stored/urls_found*100) if urls_found > 0 else 0:.1f}%
        """)
    
    def get_overall_stats(self):
        """Get overall statistics"""
        uptime = (datetime.utcnow() - self.stats['start_time']).total_seconds()
        return {
            'uptime_seconds': uptime,
            'total_urls_discovered': self.stats['urls_discovered'],
            'total_articles_crawled': self.stats['articles_crawled'],
            'total_articles_processed': self.stats['articles_processed'],
            'total_articles_stored': self.stats['articles_stored'],
            'total_errors': self.stats['errors'],
            'articles_per_hour': (self.stats['articles_stored'] / (uptime / 3600)) if uptime > 0 else 0
        }
    
    async def run_continuous(self, cycles=None):
        """Run continuous crawling"""
        logger.info("🎯 Starting Continuous Crawling Mode")
        cycle_count = 0
        
        try:
            while cycles is None or cycle_count < cycles:
                cycle_count += 1
                logger.info(f"🔄 Starting cycle {cycle_count}")
                
                await self.run_single_cycle()
                
                # Show overall stats
                stats = self.get_overall_stats()
                logger.info(f"""
📈 OVERALL STATISTICS (Cycle {cycle_count})
⏰ Uptime: {stats['uptime_seconds']/3600:.1f}h
📥 URLs Discovered: {stats['total_urls_discovered']}
📄 Articles Crawled: {stats['total_articles_crawled']}  
🔧 Articles Processed: {stats['total_articles_processed']}
💾 Articles Stored: {stats['total_articles_stored']}
❌ Errors: {stats['total_errors']}
🚀 Rate: {stats['articles_per_hour']:.1f} articles/hour
                """)
                
                # Wait for next cycle
                interval = self.config.get('crawler', {}).get('crawl_interval_seconds', 300)
                logger.info(f"⏳ Waiting {interval} seconds until next cycle...")
                await asyncio.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Crawling interrupted by user")
        except Exception as e:
            logger.error(f"❌ Continuous crawling error: {e}")
        finally:
            self.storage.close()
            logger.info("🔌 Orchestrator shutdown complete")

# Simple one-time run function
async def run_once():
    """Run one complete cycle"""
    orchestrator = OmniOrchestrator()
    await orchestrator.run_single_cycle()
    
    # Print final stats
    stats = orchestrator.get_overall_stats()
    print(f"""
🎉 SINGLE CYCLE COMPLETE!
📊 Results:
   URLs Discovered: {stats['total_urls_discovered']}
   Articles Stored: {stats['total_articles_stored']}
   Errors: {stats['total_errors']}
    """)

# Test function
async def test_orchestrator():
    """Test the orchestrator"""
    print("🧪 Testing Omni-Orchestrator...")
    
    orchestrator = OmniOrchestrator()
    await orchestrator.run_single_cycle()
    
    stats = orchestrator.get_overall_stats()
    print(f"✅ Orchestrator test complete!")
    print(f"📊 Stats: {stats['total_articles_stored']} articles stored")

if __name__ == "__main__":
    # For testing, run once
    asyncio.run(test_orchestrator())