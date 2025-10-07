"""
Simple Crawler wrapper for compatibility with the orchestrator.
This file provides the SimpleCrawler interface that the orchestrator expects.
"""

import asyncio
import aiohttp
import logging
from urllib.parse import urlparse
import time
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleCrawler:
    def __init__(self):
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def crawl_url(self, url, metadata=None):
        """Simple but robust crawler"""
        logger.info(f"🕷️ Crawling URL: {url}")
        
        try:
            start_time = time.time()
            
            async with self.session.get(url, timeout=30, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}")
                
                html = await response.text()
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title = soup.find('title')
            title_text = title.get_text().strip() if title else 'No Title'
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Try to find main content
            content_text = self._extract_content(soup, url)
            
            # If no good content found, use body as fallback
            if not content_text or len(content_text) < 200:
                body = soup.find('body')
                if body:
                    content_text = ' '.join(body.get_text().split()[:1500])
            
            result = {
                'title': title_text,
                'content': content_text,
                'authors': self._extract_authors(soup),
                'publish_date': self._extract_publish_date(soup),
                'domain': urlparse(url).netloc,
                'images': self._extract_images(soup),
                'description': self._extract_description(soup),
                'language': 'en',
                'url': url,
                'crawl_time': time.time() - start_time,
                'crawler_engine': 'simple_crawler',
                'content_length': len(content_text),
                'word_count': len(content_text.split())
            }
            
            if self._validate_article(result):
                logger.info(f"✅ Crawl successful: {len(content_text)} chars")
                return result
            else:
                logger.warning("⚠️ Insufficient content")
                return None
                
        except Exception as e:
            logger.error(f"❌ Crawl failed: {str(e)}")
            return None
    
    def _extract_content(self, soup, url):
        """Extract main article content"""
        # Common content selectors for news sites
        content_selectors = [
            'article',
            'main',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.story-content',
            '.article-body',
            '.post-body',
            '[role="main"]',
            '.content',
            '.main-content'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            for elem in elements:
                text = ' '.join(elem.get_text().split())
                if len(text) > 300:  # Substantial content
                    return text
        
        return ""
    
    def _extract_authors(self, soup):
        """Extract author information"""
        author_selectors = [
            '.author',
            '.byline',
            '[rel="author"]',
            '.article-author',
            '.post-author'
        ]
        
        authors = []
        for selector in author_selectors:
            elements = soup.select(selector)
            for elem in elements:
                author_text = elem.get_text().strip()
                if author_text and len(author_text) < 100:  # Reasonable author name length
                    authors.append(author_text)
        
        return list(set(authors))  # Remove duplicates
    
    def _extract_publish_date(self, soup):
        """Extract publish date"""
        date_selectors = [
            'time[datetime]',
            '.publish-date',
            '.post-date',
            '.article-date',
            '[property="article:published_time"]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                if element.get('datetime'):
                    return element.get('datetime')
                else:
                    return element.get_text().strip()
        
        return None
    
    def _extract_images(self, soup):
        """Extract images from article"""
        images = []
        for img in soup.find_all('img', src=True):
            src = img['src']
            if src.startswith(('http', '//')):
                images.append(src)
        
        return images[:5]  # Return first 5 images
    
    def _extract_description(self, soup):
        """Extract meta description"""
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc.get('content')
        
        # Fallback to first paragraph
        first_p = soup.find('p')
        if first_p:
            return ' '.join(first_p.get_text().split()[:50])
        
        return ""
    
    def _validate_article(self, article_data):
        """Quality control"""
        min_content_length = 200
        required_fields = ['title', 'content']
        
        # Check required fields
        if not all(field in article_data for field in required_fields):
            return False
            
        # Check content length
        if len(article_data.get('content', '')) < min_content_length:
            return False
            
        # Check title is not default
        if article_data.get('title') == 'No Title':
            return False
            
        return True

# Test function
async def test_simple_crawler():
    """Test the simple crawler"""
    print("🧪 Testing Simple Crawler...")
    
    test_urls = [
        "https://www.bbc.com/news/articles/cr5q0dr47mlo",
        "https://www.bbc.com/news/articles/c62ne93n090o"
    ]
    
    async with SimpleCrawler() as crawler:
        for url in test_urls:
            print(f"\n🔗 Testing URL: {url}")
            result = await crawler.crawl_url(url)
            
            if result:
                print(f"  ✅ SUCCESS")
                print(f"  📝 Title: {result['title'][:80]}...")
                print(f"  📊 Content: {result['content_length']} chars, {result['word_count']} words")
                print(f"  ⏱️  Time: {result['crawl_time']:.2f}s")
                if result.get('authors'):
                    print(f"  👥 Authors: {', '.join(result['authors'])}")
                if result.get('publish_date'):
                    print(f"  📅 Date: {result['publish_date']}")
            else:
                print("  ❌ FAILED")

if __name__ == "__main__":
    asyncio.run(test_simple_crawler())