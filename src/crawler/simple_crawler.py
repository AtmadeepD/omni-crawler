"""
Simple Crawler wrapper for compatibility with the orchestrator.
This file provides the SimpleCrawler interface that the orchestrator expects.
"""

import asyncio
import aiohttp
import logging
from urllib.parse import urlparse
import time
from datetime import datetime
import hashlib
import sys
import os

# Add the src directory to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.content_cleaner import clean_content

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleCrawler:
    def __init__(self):
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def crawl_url(self, url, metadata=None):
        """Simple but robust crawler with enhanced content cleaning"""
        logger.info(f"🕷️ Crawling URL: {url}")
        
        try:
            start_time = time.time()
            
            async with self.session.get(url, timeout=30, headers=self.headers) as response:
                if response.status != 200:
                    logger.error(f"❌ HTTP {response.status} for {url}")
                    return None
                
                html_content = await response.text()
            
            # Use enhanced content cleaning
            cleaned_content = clean_content(html_content, url)
            
            # Validate we got substantial content
            if not cleaned_content or len(cleaned_content.strip()) < 100:
                logger.warning(f"⚠️ Insufficient content from {url}")
                return None
            
            # Extract title and metadata
            title = self._extract_title(html_content, url)
            domain = self._extract_domain(url)
            
            # Create article data
            article_data = {
                'article_id': self._generate_article_id(url, html_content),
                'title': title,
                'url': url,
                'domain': domain,
                'content': cleaned_content,
                'content_length': len(cleaned_content),
                'processing_timestamp': datetime.utcnow().isoformat(),
                'crawler_engine': 'simple_crawler_enhanced',
                'authors': self._extract_authors_basic(html_content),
                'publish_date': self._extract_publish_date_basic(html_content),
                'word_count': len(cleaned_content.split())
            }
            
            # Add metadata if provided
            if metadata:
                article_data.update(metadata)
            
            # Final validation
            if self._validate_article(article_data):
                logger.info(f"✅ Crawl successful: {len(cleaned_content)} chars, {article_data['word_count']} words")
                return article_data
            else:
                logger.warning(f"⚠️ Article validation failed for {url}")
                return None
                
        except asyncio.TimeoutError:
            logger.error(f"❌ Timeout crawling {url}")
            return None
        except Exception as e:
            logger.error(f"❌ Crawl failed for {url}: {str(e)}")
            return None
    
    def _extract_title(self, html_content: str, url: str) -> str:
        """Extract title from HTML"""
        try:
            import re
            # Simple regex to extract title
            title_match = re.search(r'<title[^>]*>(.*?)</title>', html_content, re.IGNORECASE | re.DOTALL)
            if title_match:
                title = title_match.group(1).strip()
                # Clean the title
                title = re.sub(r'\s+', ' ', title)
                if title and title != 'No Title':
                    return title[:500]  # Limit title length
        except Exception as e:
            logger.debug(f"Title extraction failed: {e}")
        
        # Fallback: use domain or URL
        domain = self._extract_domain(url)
        return f"Article from {domain}"
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return "unknown"
    
    def _generate_article_id(self, url: str, content: str) -> str:
        """Generate unique article ID"""
        try:
            # Use URL + content hash for uniqueness
            content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:8]
            url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
            return f"article_{url_hash}_{content_hash}"
        except:
            # Fallback: timestamp-based ID
            return f"article_{int(time.time())}"
    
    def _extract_authors_basic(self, html_content: str) -> list:
        """Basic author extraction"""
        try:
            import re
            # Common author patterns
            author_patterns = [
                r'"author"[^>]*content="([^"]+)"',
                r'class="author"[^>]*>([^<]+)',
                r'byline"[^>]*>([^<]+)',
            ]
            
            authors = []
            for pattern in author_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                for match in matches:
                    if match and len(match) < 100:  # Reasonable author name length
                        authors.append(match.strip())
            
            return list(set(authors))  # Remove duplicates
        except:
            return []
    
    def _extract_publish_date_basic(self, html_content: str) -> str:
        """Basic publish date extraction"""
        try:
            import re
            # Common date patterns
            date_patterns = [
                r'"published_time"[^>]*content="([^"]+)"',
                r'"datePublished"[^>]*content="([^"]+)"',
                r'datetime="([^"]+)"',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    return match.group(1)
            
            return datetime.utcnow().isoformat()
        except:
            return datetime.utcnow().isoformat()
    
    def _validate_article(self, article_data: dict) -> bool:
        """Quality control for articles"""
        min_content_length = 200
        min_word_count = 50
        required_fields = ['title', 'content', 'url', 'domain']
        
        # Check required fields
        if not all(field in article_data for field in required_fields):
            logger.warning("Missing required fields")
            return False
            
        # Check content length
        if len(article_data.get('content', '')) < min_content_length:
            logger.warning(f"Content too short: {len(article_data.get('content', ''))} chars")
            return False
            
        # Check word count
        if article_data.get('word_count', 0) < min_word_count:
            logger.warning(f"Too few words: {article_data.get('word_count', 0)} words")
            return False
            
        # Check title is not default
        if article_data.get('title', '').startswith('Article from'):
            logger.warning("Using default title")
            return False
            
        return True

    def save_article(self, article_data):
        """Save article with enhanced processing"""
        try:
            # Use the enhanced storage system
            result = self.storage.save_enhanced_article(article_data)
            
            if result['success']:
                logger.info(f"✅ Enhanced article saved: {result['article_id']}")
                return True
            else:
                logger.error(f"❌ Failed to save enhanced article: {result.get('error')}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Article save failed: {e}")
            return False

# Test function
async def test_simple_crawler():
    """Test the simple crawler with enhanced content cleaning"""
    print("🧪 Testing Enhanced Simple Crawler...")
    
    # Test with URLs that we know work
    test_urls = [
        "https://www.bbc.com/news/articles/cvgqyj268ljo",  # BBC article that worked
        "https://www.npr.org/2025/10/06/nx-s1-5560216/who-is-larry-ellison-the-billionaire-trump-friend-whos-part-of-the-tikktok-takeover",
    ]
    
    async with SimpleCrawler() as crawler:
        for url in test_urls:
            print(f"\n🔗 Testing URL: {url}")
            result = await crawler.crawl_url(url)
            
            if result:
                print(f"  ✅ SUCCESS")
                print(f"  📝 Title: {result['title'][:80]}...")
                print(f"  📊 Content: {result['content_length']} chars, {result['word_count']} words")
                print(f"  🌐 Domain: {result['domain']}")
                print(f"  🔗 URL: {result['url']}")
                print(f"  📅 Processing: {result['processing_timestamp']}")
                
                # Show sample of cleaned content
                content_sample = result['content'][:200] + "..." if len(result['content']) > 200 else result['content']
                print(f"  📖 Content sample: {content_sample}")
                
                if result.get('authors'):
                    print(f"  👥 Authors: {', '.join(result['authors'])}")
            else:
                print("  ❌ FAILED")

if __name__ == "__main__":
    asyncio.run(test_simple_crawler())