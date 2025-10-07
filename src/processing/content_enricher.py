import re
import nltk
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
import logging
from collections import Counter

# Download required NLTK data (run once)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

logger = logging.getLogger(__name__)

class ContentEnricher:
    """Enhanced content processing and enrichment"""
    
    def __init__(self):
        self.stop_words = set(nltk.corpus.stopwords.words('english'))
    
    def enrich_article(self, basic_article: Dict) -> Dict:
        """Comprehensive article enrichment"""
        enriched_data = {}
        
        try:
            # Extract excerpt and summary
            enriched_data.update(self._generate_summary(basic_article))
            
            # Extract entities
            enriched_data['entities'] = self._extract_entities(basic_article)
            
            # Extract keywords
            enriched_data['keywords'] = self._extract_keywords(basic_article)
            
            # Detect language
            enriched_data['language'] = self._detect_language(basic_article)
            
            # Calculate read time
            enriched_data['read_time'] = self._calculate_read_time(basic_article)
            
            # Enhanced sentiment analysis
            enriched_data['sentiment'] = self._analyze_sentiment(basic_article)
            
            # Extract topics
            enriched_data['topics'] = self._extract_topics(basic_article)
            
            # Generate content hash
            enriched_data['content_hash'] = self._generate_content_hash(basic_article)
            
            # Calculate confidence score
            enriched_data['confidence_score'] = self._calculate_confidence(basic_article, enriched_data)
            
            logger.info(f"Successfully enriched article: {basic_article.get('title', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Article enrichment failed: {str(e)}")
            # Return minimal enrichment to avoid complete failure
            enriched_data = {
                'excerpt': self._truncate_text(basic_article.get('content', ''), 200),
                'keywords': [],
                'language': 'en',
                'read_time': 0,
                'sentiment': {'overall': 0.0},
                'topics': [],
                'content_hash': '',
                'confidence_score': 0.5
            }
        
        return enriched_data
    
    def _generate_summary(self, article: Dict) -> Dict:
        """Generate excerpt and summary from content"""
        content = article.get('content', '')
        title = article.get('title', '')
        
        # Simple excerpt (first 200 chars)
        excerpt = self._truncate_text(content, 200)
        
        # Slightly more sophisticated summary
        sentences = nltk.sent_tokenize(content)
        if len(sentences) >= 3:
            summary = ' '.join(sentences[:3])  # First 3 sentences
        else:
            summary = content[:500]  # Fallback
        
        return {
            'excerpt': excerpt,
            'summary': summary.strip()
        }
    
    def _extract_entities(self, article: Dict) -> Dict[str, List]:
        """Basic entity extraction (people, organizations, locations)"""
        content = f"{article.get('title', '')} {article.get('content', '')}"
        
        # Simple regex-based entity extraction
        # In production, replace with spaCy or similar NLP library
        entities = {
            'people': self._extract_people(content),
            'organizations': self._extract_organizations(content),
            'locations': self._extract_locations(content)
        }
        
        return entities
    
    def _extract_people(self, text: str) -> List[Dict]:
        """Extract person names using simple patterns"""
        # Basic pattern for capitalized words (very simplistic)
        people = []
        words = text.split()
        
        for i, word in enumerate(words):
            if (word.istitle() and len(word) > 1 and 
                word.lower() not in self.stop_words and
                (i == 0 or words[i-1][-1] not in ['.', '!', '?'])):
                
                # Check if this might be part of a multi-word name
                if (i + 1 < len(words) and words[i + 1].istitle() and
                    len(words[i + 1]) > 1):
                    
                    full_name = f"{word} {words[i + 1]}"
                    people.append({'name': full_name, 'confidence': 0.6})
                else:
                    people.append({'name': word, 'confidence': 0.3})
        
        return people[:10]  # Limit to top 10
    
    def _extract_organizations(self, text: str) -> List[Dict]:
        """Extract organization names"""
        orgs = []
        
        # Look for company suffixes
        org_patterns = [
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(Inc|Corp|Corporation|Company|Co|LLC|Ltd)\b',
            r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(International|Global|Technologies|Systems)\b'
        ]
        
        for pattern in org_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                orgs.append({'name': match.group(0), 'confidence': 0.7})
        
        return orgs[:10]
    
    def _extract_locations(self, text: str) -> List[Dict]:
        """Extract location names"""
        locations = []
        
        # Simple pattern for locations (capitalized after certain prepositions)
        location_pattern = r'\b(in|at|from|to)\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*)\b'
        
        matches = re.finditer(location_pattern, text, re.IGNORECASE)
        for match in matches:
            locations.append({'name': match.group(2), 'confidence': 0.5})
        
        return locations[:10]
    
    def _extract_keywords(self, article: Dict, max_keywords: int = 15) -> List[str]:
        """Extract important keywords from content"""
        content = f"{article.get('title', '')} {article.get('content', '')}"
        
        # Tokenize and clean
        words = re.findall(r'\b[a-zA-Z]{3,}\b', content.lower())
        
        # Remove stop words and count frequency
        filtered_words = [word for word in words if word not in self.stop_words]
        word_freq = Counter(filtered_words)
        
        # Get most common words
        keywords = [word for word, count in word_freq.most_common(max_keywords)]
        
        return keywords
    
    def _detect_language(self, article: Dict) -> str:
        """Simple language detection (basic implementation)"""
        content = article.get('content', '').lower()
        
        # Simple English detection based on common words
        english_words = {'the', 'and', 'of', 'to', 'a', 'in', 'is', 'you', 'that', 'it'}
        words = set(re.findall(r'\b[a-z]{2,}\b', content))
        
        english_matches = len(words.intersection(english_words))
        
        if english_matches > 5:
            return 'en'
        else:
            return 'unknown'
    
    def _calculate_read_time(self, article: Dict) -> int:
        """Calculate estimated read time in minutes"""
        content = article.get('content', '')
        words_per_minute = 200  # Average reading speed
        
        word_count = len(re.findall(r'\b\w+\b', content))
        read_time = max(1, round(word_count / words_per_minute))
        
        return read_time
    
    def _analyze_sentiment(self, article: Dict) -> Dict[str, float]:
        """Enhanced sentiment analysis"""
        content = article.get('content', '')
        
        # Simple sentiment word lists (expand in production)
        positive_words = {'good', 'great', 'excellent', 'amazing', 'wonderful', 'best', 'fantastic'}
        negative_words = {'bad', 'terrible', 'awful', 'horrible', 'worst', 'negative', 'poor'}
        
        words = set(re.findall(r'\b\w+\b', content.lower()))
        
        positive_count = len(words.intersection(positive_words))
        negative_count = len(words.intersection(negative_words))
        total_sentiment_words = positive_count + negative_count
        
        if total_sentiment_words > 0:
            sentiment_score = (positive_count - negative_count) / total_sentiment_words
        else:
            sentiment_score = 0.0
        
        return {
            'overall': sentiment_score,
            'positive': positive_count,
            'negative': negative_count,
            'neutral': len(words) - total_sentiment_words
        }
    
    def _extract_topics(self, article: Dict) -> List[str]:
        """Extract broad topics from content"""
        content = article.get('content', '').lower()
        title = article.get('title', '').lower()
        
        # Topic keywords mapping (expand this extensively)
        topic_keywords = {
            'technology': ['ai', 'artificial intelligence', 'software', 'tech', 'digital', 'computer'],
            'politics': ['government', 'election', 'policy', 'political', 'senate', 'congress'],
            'business': ['market', 'economy', 'company', 'business', 'financial', 'stock'],
            'sports': ['game', 'team', 'player', 'sport', 'championship', 'score'],
            'health': ['medical', 'health', 'disease', 'hospital', 'medicine', 'treatment']
        }
        
        detected_topics = []
        full_text = f"{title} {content}"
        
        for topic, keywords in topic_keywords.items():
            if any(keyword in full_text for keyword in keywords):
                detected_topics.append(topic)
        
        return detected_topics[:3]  # Limit to top 3 topics
    
    def _generate_content_hash(self, article: Dict) -> str:
        """Generate content hash for duplicate detection"""
        import hashlib
        
        content_to_hash = f"{article.get('title', '')}{article.get('content', '')}{article.get('url', '')}"
        return hashlib.sha256(content_to_hash.encode('utf-8')).hexdigest()
    
    def _calculate_confidence(self, article: Dict, enriched_data: Dict) -> float:
        """Calculate confidence score for enrichment results"""
        confidence_factors = []
        
        # Content length factor
        content_len = len(article.get('content', ''))
        if content_len > 1000:
            confidence_factors.append(0.9)
        elif content_len > 500:
            confidence_factors.append(0.7)
        elif content_len > 100:
            confidence_factors.append(0.5)
        else:
            confidence_factors.append(0.3)
        
        # Entity extraction factor
        entities = enriched_data.get('entities', {})
        total_entities = sum(len(entities.get(key, [])) for key in entities)
        if total_entities > 5:
            confidence_factors.append(0.8)
        elif total_entities > 2:
            confidence_factors.append(0.6)
        else:
            confidence_factors.append(0.4)
        
        # Keyword extraction factor
        keywords = enriched_data.get('keywords', [])
        if len(keywords) >= 5:
            confidence_factors.append(0.7)
        elif len(keywords) >= 2:
            confidence_factors.append(0.5)
        else:
            confidence_factors.append(0.3)
        
        # Average all factors
        avg_confidence = sum(confidence_factors) / len(confidence_factors) if confidence_factors else 0.5
        
        return round(avg_confidence, 2)
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Truncate text to specified length"""
        if len(text) <= max_length:
            return text
        return text[:max_length].rsplit(' ', 1)[0] + '...'