import asyncio
import aiohttp
import feedparser
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse
import redis
import hashlib
import yaml
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OmniURLDiscoverer:
    def __init__(self, redis_host='redis', redis_port=6379):
        try:
            self.redis = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
            self.redis.ping()  # Test connection
            logger.info("✅ Redis connection successful")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise
        
        self.visited_urls_key = "omni:crawler:visited_urls"
        
        # Load configuration
        with open('config/discovery_sources.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
    
    async def discover_from_rss(self, rss_url):
        """Primary method: RSS feeds - most efficient"""
        logger.info(f"🔍 Discovering URLs from RSS: {rss_url}")
        try:
            # Use async feedparser
            feed = feedparser.parse(rss_url)
            discovered = []
            
            for entry in feed.entries:
                url = entry.link
                if not self._is_visited(url):
                    discovered.append({
                        'url': url,
                        'title': entry.title,
                        'published': entry.get('published', ''),
                        'source': 'rss',
                        'priority': 10,  # High priority - known to be fresh content
                        'discovery_timestamp': datetime.utcnow().isoformat()
                    })
                    logger.info(f"📰 Found new article: {entry.title[:50]}...")
            
            logger.info(f"✅ RSS discovery complete: {len(discovered)} new URLs")
            return discovered
        except Exception as e:
            logger.error(f"❌ RSS Error {rss_url}: {e}")
            return []
    
    async def discover_from_sitemap(self, sitemap_url):
        """Secondary method: Sitemaps - comprehensive but slower"""
        logger.info(f"🔍 Discovering URLs from sitemap: {sitemap_url}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(sitemap_url, timeout=30) as response:
                    if response.status != 200:
                        logger.warning(f"⚠️ Sitemap not accessible: {sitemap_url} (Status: {response.status})")
                        return []
                    content = await response.text()
                    
            root = ET.fromstring(content)
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            discovered = []
            for url_elem in root.findall('.//ns:url', namespace):
                loc = url_elem.find('ns:loc', namespace)
                if loc is not None:
                    url = loc.text
                    if not self._is_visited(url):
                        discovered.append({
                            'url': url,
                            'source': 'sitemap', 
                            'priority': 5,  # Medium priority
                            'discovery_timestamp': datetime.utcnow().isoformat()
                        })
            
            logger.info(f"✅ Sitemap discovery complete: {len(discovered)} new URLs")
            return discovered
        except Exception as e:
            logger.error(f"❌ Sitemap Error {sitemap_url}: {e}")
            return []
    
    async def run_discovery(self):
        """Run complete discovery process"""
        logger.info("🚀 Starting URL discovery process...")
        all_urls = []
        
        # Discover from RSS feeds
        for feed_config in self.config.get('rss_feeds', []):
            if feed_config.get('enabled', True):
                urls = await self.discover_from_rss(feed_config['url'])
                all_urls.extend(urls)
        
        # Sort by priority (highest first)
        all_urls.sort(key=lambda x: x['priority'], reverse=True)
        
        logger.info(f"🎯 Discovery complete: Total {len(all_urls)} URLs found")
        return all_urls
    
    def _is_visited(self, url):
        """Bloom filter-like functionality using Redis"""
        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()
            return self.redis.sismember(self.visited_urls_key, url_hash)
        except Exception as e:
            logger.error(f"❌ Redis error in _is_visited: {e}")
            return False
    
    def _mark_visited(self, url):
        """Mark URL as visited"""
        try:
            url_hash = hashlib.md5(url.encode()).hexdigest()
            self.redis.sadd(self.visited_urls_key, url_hash)
        except Exception as e:
            logger.error(f"❌ Redis error in _mark_visited: {e}")

# Test function
async def test_discovery():
    """Test the discovery engine"""
    print("🧪 Testing URL Discovery Engine...")
    
    discoverer = OmniURLDiscoverer(redis_host='localhost')
    urls = await discoverer.run_discovery()
    
    print(f"📊 Discovery Results: {len(urls)} URLs found")
    for i, url_info in enumerate(urls[:5]):  # Show first 5
        print(f"  {i+1}. {url_info['title'][:60]}...")
        print(f"     URL: {url_info['url']}")
        print(f"     Source: {url_info['source']}")
        print()

if __name__ == "__main__":
    asyncio.run(test_discovery())