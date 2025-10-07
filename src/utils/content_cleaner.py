import re
import html
from bs4 import BeautifulSoup
import logging
from typing import Optional
import urllib.parse

logger = logging.getLogger(__name__)

class ContentCleaner:
    """Advanced content cleaning and extraction with domain-specific rules"""
    
    def __init__(self):
        self.unwanted_patterns = [
            r'<!--.*?-->',  # HTML comments
            r'<script.*?>.*?</script>',  # Script tags
            r'<style.*?>.*?</style>',  # Style tags
            r'<nav.*?>.*?</nav>',  # Navigation
            r'<header.*?>.*?</header>',  # Header
            r'<footer.*?>.*?</footer>',  # Footer
            r'<aside.*?>.*?</aside>',  # Sidebar
            r'<iframe.*?>.*?</iframe>',  # Iframes
            r'ADVERTISEMENT',  # Ads
            r'Sign up for.*?newsletter',  # Newsletter prompts
            r'Follow us on',  # Social media prompts
            r'Download Embed',  # NPR download text
            r'Listen · \d+:\d+',  # NPR listen time
            r'Transcript',  # NPR transcript text
        ]
        
        # Domain-specific content selectors
        self.domain_selectors = {
            'bbc.com': [
                '[data-component="text-block"]',
                '.ssrcss-1q0x1qg-Paragraph',
                '.story-body__inner',
                '[role="main"]'
            ],
            'bbc.co.uk': [
                '[data-component="text-block"]',
                '.ssrcss-1q0x1qg-Paragraph',
                '.story-body__inner',
                '[role="main"]'
            ],
            'npr.org': [
                '.storytext',
                '.transcript > p',
                '[data-story="true"] p',
                '.storycontent p'
            ],
            'reuters.com': [
                '.ArticleBody__container',
                '.StandardArticleBody_body',
                'article p'
            ],
            'cnn.com': [
                '.article__content',
                '.zn-body-text',
                'article p'
            ]
        }
    
    def clean_html_content(self, html_content: str, url: str = "") -> str:
        """
        Extract clean, readable text from HTML content with domain-specific rules
        """
        if not html_content or not html_content.strip():
            return ""
        
        try:
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove unwanted elements
            self._remove_unwanted_elements(soup)
            
            # Extract main content based on domain
            main_content = self._extract_main_content(soup, url)
            
            if not main_content or len(main_content.strip()) < 100:
                # Fallback: try generic content extraction
                main_content = self._extract_generic_content(soup)
            
            # Clean the text
            cleaned_text = self._clean_text(main_content)
            
            # Final validation
            if self._is_valid_content(cleaned_text, url):
                logger.info(f"✅ Content cleaned: {len(cleaned_text)} chars from {len(html_content)} raw")
                return cleaned_text
            else:
                logger.warning(f"⚠️ Content validation failed for {url}")
                return ""
            
        except Exception as e:
            logger.error(f"❌ HTML cleaning failed for {url}: {e}")
            # Fallback: basic cleaning
            return self._basic_clean(html_content)
    
    def _remove_unwanted_elements(self, soup: BeautifulSoup):
        """Remove unwanted HTML elements"""
        unwanted_tags = [
            'script', 'style', 'nav', 'header', 'footer', 'aside',
            'meta', 'link', 'button', 'form', 'iframe', 'noscript',
            'svg', 'path', 'img', 'audio', 'video', 'source'
        ]
        
        for tag in unwanted_tags:
            for element in soup.find_all(tag):
                element.decompose()
        
        # Remove elements with common ad classes/ids
        ad_indicators = [
            'advertisement', 'ad-container', 'banner-ad', 'popup',
            'newsletter', 'subscribe', 'social-share', 'comments',
            'share', 'related', 'recommended', 'popular', 'trending'
        ]
        
        for indicator in ad_indicators:
            # By class
            for element in soup.find_all(class_=re.compile(indicator, re.I)):
                element.decompose()
            # By id
            for element in soup.find_all(id=re.compile(indicator, re.I)):
                element.decompose()
    
    def _extract_main_content(self, soup: BeautifulSoup, url: str) -> Optional[str]:
        """
        Extract main article content using domain-specific rules
        """
        domain = self._extract_domain(url)
        
        # Try domain-specific selectors first
        if domain in self.domain_selectors:
            for selector in self.domain_selectors[domain]:
                elements = soup.select(selector)
                content_parts = []
                
                for elem in elements:
                    text = self._clean_element_text(elem)
                    if text and len(text) > 50:  # Substantial content
                        content_parts.append(text)
                
                if content_parts:
                    combined = ' '.join(content_parts)
                    if len(combined) > 200:
                        return combined
        
        # Fallback to generic content extraction
        return self._extract_generic_content(soup)
    
    def _extract_generic_content(self, soup: BeautifulSoup) -> Optional[str]:
        """Generic content extraction as fallback"""
        # Try common article content selectors
        content_selectors = [
            'article p',
            'main p',
            '.article-content p',
            '.post-content p',
            '.entry-content p',
            '.story-content p',
            '.article-body p',
            '.post-body p',
            '[role="main"] p',
            '.content p',
            '.main-content p'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            content_parts = []
            
            for elem in elements:
                text = self._clean_element_text(elem)
                if text and len(text) > 20:  # Reasonable paragraph
                    content_parts.append(text)
            
            if content_parts:
                combined = ' '.join(content_parts)
                if len(combined) > 200:
                    return combined
        
        # Last resort: get all paragraphs and filter
        all_paragraphs = soup.find_all('p')
        content_parts = []
        
        for p in all_paragraphs:
            text = self._clean_element_text(p)
            if text and len(text) > 50 and len(text) < 1000:  # Reasonable paragraph size
                content_parts.append(text)
        
        if content_parts:
            return ' '.join(content_parts)
        
        return None
    
    def _clean_element_text(self, element) -> str:
        """Clean text from a single element"""
        if not element:
            return ""
        
        # Get text and clean it
        text = element.get_text()
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        return text
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urllib.parse.urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return ""
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize extracted text"""
        if not text:
            return ""
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove unwanted patterns
        for pattern in self.unwanted_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # Normalize whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def _is_valid_content(self, text: str, url: str) -> bool:
        """Validate if content is actually readable article content"""
        if not text or len(text) < 100:
            return False
        
        # Check for common error pages
        error_indicators = [
            'page not found',
            'sorry, we couldn\'t find that page',
            'error 404',
            'not found',
            'this page doesn\'t exist'
        ]
        
        text_lower = text.lower()
        if any(indicator in text_lower for indicator in error_indicators):
            return False
        
        # Check for reasonable sentence structure
        sentences = text.split('. ')
        if len(sentences) < 2:
            return False
        
        # Check average sentence length
        avg_sentence_length = sum(len(sentence.split()) for sentence in sentences) / len(sentences)
        if avg_sentence_length < 3 or avg_sentence_length > 50:
            return False
        
        return True
    
    def _basic_clean(self, html_content: str) -> str:
        """Basic fallback cleaning without BeautifulSoup"""
        try:
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', html_content)
            # Decode HTML entities
            text = html.unescape(text)
            # Normalize whitespace
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception as e:
            logger.error(f"Basic cleaning failed: {e}")
            return html_content[:5000]  # Return first 5000 chars as fallback

# Global instance
content_cleaner = ContentCleaner()

def clean_content(html_content: str, url: str = "") -> str:
    """Convenience function for content cleaning"""
    return content_cleaner.clean_html_content(html_content, url)