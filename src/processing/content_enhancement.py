import spacy
from textblob import TextBlob
import nltk
import hashlib
import logging
from datetime import datetime
import re

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentEnhancementPipeline:
    def __init__(self):
        try:
            self.nlp = spacy.load("en_core_web_sm")
            logger.info("✅ spaCy model loaded successfully")
        except OSError:
            logger.warning("⚠️ spaCy model not found. Installing...")
            import os
            os.system("python -m spacy download en_core_web_sm")
            self.nlp = spacy.load("en_core_web_sm")
        
    def process_article(self, raw_article):
        """Enhance raw article with NLP and metadata"""
        logger.info(f"🔧 Processing article: {raw_article['title'][:50]}...")
        
        enhanced = raw_article.copy()
        
        # Generate unique ID
        enhanced['article_id'] = self._generate_article_id(raw_article)
        enhanced['processing_timestamp'] = datetime.utcnow().isoformat()
        
        # NLP Processing
        enhanced.update(self._extract_entities(raw_article['content']))
        enhanced.update(self._analyze_sentiment(raw_article['content']))
        enhanced.update(self._extract_key_phrases(raw_article['content']))
        
        # Quality scoring
        enhanced['quality_score'] = self._calculate_quality_score(enhanced)
        
        # Categorization
        enhanced['category'] = self._categorize_article(enhanced)
        
        # Ensure all required fields for database
        enhanced['sentiment_label'] = enhanced.get('sentiment_label', 'neutral')
        enhanced['content_length'] = len(raw_article.get('content', ''))
        enhanced['crawler_engine'] = raw_article.get('crawler_engine', 'simple_crawler')
        
        logger.info(f"✅ Article processed: {enhanced['quality_score']}/100 quality score")
        return enhanced
    
    def _extract_entities(self, text):
        """Extract people, organizations, locations"""
        if not text or len(text) < 50:
            return {
                'entities': {
                    'persons': [],
                    'organizations': [], 
                    'locations': [],
                    'dates': []
                }
            }
        
        try:
            doc = self.nlp(text[:100000])  # Limit text length for performance
            
            entities = {
                'persons': [],
                'organizations': [], 
                'locations': [],
                'dates': []
            }
            
            for ent in doc.ents:
                if ent.label_ == 'PERSON' and len(ent.text) > 1:
                    entities['persons'].append(ent.text)
                elif ent.label_ == 'ORG' and len(ent.text) > 1:
                    entities['organizations'].append(ent.text)
                elif ent.label_ == 'GPE' and len(ent.text) > 1:
                    entities['locations'].append(ent.text)
                elif ent.label_ == 'DATE' and len(ent.text) > 1:
                    entities['dates'].append(ent.text)
            
            # Remove duplicates and limit to top 10 each
            for key in entities:
                entities[key] = list(set(entities[key]))[:10]
                
            return {'entities': entities}
        except Exception as e:
            logger.error(f"Entity extraction error: {e}")
            return {'entities': {'persons': [], 'organizations': [], 'locations': [], 'dates': []}}
    
    def _analyze_sentiment(self, text):
        """Multi-level sentiment analysis"""
        try:
            if not text:
                return {
                    'sentiment_polarity': 0,
                    'sentiment_subjectivity': 0,
                    'sentiment_label': 'neutral'
                }
                
            blob = TextBlob(text)
            
            polarity = blob.sentiment.polarity
            if polarity > 0.1:
                label = 'positive'
            elif polarity < -0.1:
                label = 'negative'
            else:
                label = 'neutral'
            
            return {
                'sentiment_polarity': polarity,
                'sentiment_subjectivity': blob.sentiment.subjectivity,
                'sentiment_label': label
            }
        except Exception as e:
            logger.error(f"Sentiment analysis error: {e}")
            return {
                'sentiment_polarity': 0,
                'sentiment_subjectivity': 0,
                'sentiment_label': 'neutral'
            }
    
    def _extract_key_phrases(self, text):
        """Extract key phrases using simple NLP"""
        try:
            if not text:
                return {'key_phrases': []}
                
            # Simple approach: use noun phrases
            doc = self.nlp(text[:5000])  # Limit for performance
            
            key_phrases = []
            for chunk in doc.noun_chunks:
                phrase = chunk.text.strip()
                if len(phrase) > 10 and len(phrase) < 50:  # Reasonable length
                    key_phrases.append(phrase)
            
            # Remove duplicates and limit
            key_phrases = list(set(key_phrases))[:15]
            
            return {'key_phrases': key_phrases}
        except Exception as e:
            logger.error(f"Key phrase extraction error: {e}")
            return {'key_phrases': []}
    
    def _calculate_quality_score(self, article):
        """Score article quality 0-100"""
        score = 0
        
        try:
            # Content length (max 30 points)
            content_length = len(article.get('content', ''))
            if content_length > 2000:
                score += 30
            elif content_length > 1000:
                score += 25
            elif content_length > 500:
                score += 20
            elif content_length > 200:
                score += 10
            
            # Title quality (max 20 points)
            title = article.get('title', '')
            if title and title != 'No Title':
                if len(title) > 20:
                    score += 20
                elif len(title) > 10:
                    score += 15
            
            # Entity richness (max 30 points)
            entities = article.get('entities', {})
            total_entities = sum(len(entities.get(key, [])) for key in ['persons', 'organizations', 'locations'])
            if total_entities > 10:
                score += 30
            elif total_entities > 5:
                score += 20
            elif total_entities > 2:
                score += 10
            
            # Author credibility (max 10 points)
            if article.get('authors'):
                score += 10
            
            # Image presence (max 10 points)
            if article.get('images'):
                score += 10
                
        except Exception as e:
            logger.error(f"Quality scoring error: {e}")
            
        return min(score, 100)
    
    def _categorize_article(self, article):
        """Simple category detection based on keywords"""
        content = (article.get('title', '') + ' ' + article.get('content', '')).lower()
        
        categories = {
            'politics': ['election', 'government', 'president', 'minister', 'congress', 'senate', 'vote'],
            'sports': ['game', 'team', 'player', 'score', 'championship', 'tournament', 'olympics'],
            'technology': ['tech', 'software', 'computer', 'digital', 'ai', 'artificial intelligence', 'robot'],
            'business': ['market', 'stock', 'economy', 'business', 'company', 'profit', 'investment'],
            'health': ['health', 'medical', 'doctor', 'hospital', 'disease', 'medicine', 'vaccine'],
            'entertainment': ['movie', 'film', 'celebrity', 'music', 'show', 'entertainment', 'actor']
        }
        
        for category, keywords in categories.items():
            if any(keyword in content for keyword in keywords):
                return category
        
        return 'general'
    
    def _generate_article_id(self, article):
        """Create unique, reproducible article ID"""
        content_hash = hashlib.sha256(
            f"{article['url']}{article['title']}".encode()
        ).hexdigest()[:16]
        return f"article_{content_hash}"

# Test function
def test_processing():
    """Test the processing pipeline"""
    print("🧪 Testing Content Enhancement Pipeline...")
    
    # Sample article data
    sample_article = {
        'title': 'Queen leads tributes to wonderfully witty friend Dame Jilly Cooper',
        'content': '''The Queen has led tributes to author Dame Jilly Cooper, describing her as a "wonderfully witty friend" who brought joy to millions through her writing. 
        
        Dame Jilly, known for her romantic novels and sharp social commentary, passed away at the age of 85. Her works included the famous "Riders" series and numerous bestsellers that captivated readers worldwide.
        
        Buckingham Palace released a statement praising her contributions to literature and her charitable work. The Prime Minister and other political figures also expressed their condolences.''',
        'url': 'https://www.bbc.com/news/articles/cr5q0dr47mlo',
        'authors': ['BBC News'],
        'domain': 'www.bbc.com'
    }
    
    processor = ContentEnhancementPipeline()
    enhanced = processor.process_article(sample_article)
    
    print(f"✅ Processing complete!")
    print(f"📊 Quality score: {enhanced['quality_score']}/100")
    print(f"🏷️  Category: {enhanced['category']}")
    print(f"😊 Sentiment: {enhanced['sentiment_label']} (polarity: {enhanced['sentiment_polarity']:.2f})")
    print(f"👥 Entities found:")
    print(f"   People: {enhanced['entities']['persons']}")
    print(f"   Organizations: {enhanced['entities']['organizations']}")
    print(f"   Locations: {enhanced['entities']['locations']}")
    print(f"   Key phrases: {enhanced['key_phrases'][:5]}...")

if __name__ == "__main__":
    test_processing()