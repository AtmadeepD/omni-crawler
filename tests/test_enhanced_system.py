import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models.enhanced_article import EnhancedArticle
from src.validation.article_validator import ArticleValidator
from src.processing.content_enricher import ContentEnricher

def test_enhanced_system():
    """Test the enhanced article processing pipeline"""
    
    # Sample article data
    sample_article = {
        'article_id': 'test-123',
        'title': 'Tesla Announces Breakthrough in Battery Technology',
        'url': 'https://example.com/tesla-battery-breakthrough',
        'domain': 'example.com',
        'authors': ['John Smith', 'Jane Doe'],
        'category': 'technology',
        'quality_score': 0.85,
        'sentiment': {'overall': 0.7},
        'content': '''
        Tesla has announced a major breakthrough in battery technology that could 
        revolutionize the electric vehicle industry. The new battery design promises 
        to double the range of current models while reducing charging time by 50%. 
        CEO Elon Musk revealed the technology at Tesla\'s annual shareholder meeting 
        in California. The innovation uses new lithium-ion chemistry developed by 
        Tesla\'s research team in partnership with Panasonic. This development comes 
        at a crucial time as competitors like Ford and GM are accelerating their 
        electric vehicle programs. Industry analysts predict this could give Tesla 
        a significant advantage in the growing EV market.
        ''',
        'content_length': 500,
        'processing_timestamp': '2024-01-15T10:30:00',
        'publish_date': '2024-01-15T08:00:00',
        'crawler_engine': 'advanced_crawler'
    }
    
    print("=== Testing Enhanced Article System ===\n")
    
    # Test Validator
    print("1. Testing Article Validator...")
    validator = ArticleValidator()
    is_valid, results = validator.validate_article(sample_article)
    print(f"   Valid: {is_valid}")
    print(f"   Quality Score: {results['quality_score']}")
    print(f"   Errors: {results['errors']}")
    print(f"   Warnings: {results['warnings']}\n")
    
    # Test Enricher
    print("2. Testing Content Enricher...")
    enricher = ContentEnricher()
    enriched_data = enricher.enrich_article(sample_article)
    print(f"   Excerpt: {enriched_data.get('excerpt', '')[:100]}...")
    print(f"   Keywords: {enriched_data.get('keywords', [])[:5]}")
    print(f"   Read Time: {enriched_data.get('read_time')} minutes")
    print(f"   Entities Found: {sum(len(v) for v in enriched_data.get('entities', {}).values())}\n")
    
    # Test Enhanced Article Model
    print("3. Testing Enhanced Article Model...")
    enhanced_article = EnhancedArticle.from_basic_article(sample_article, enriched_data)
    print(f"   Article ID: {enhanced_article.article_id}")
    print(f"   Content Hash: {enhanced_article.content_hash[:20]}...")
    print(f"   Confidence Score: {enhanced_article.confidence_score}")
    print(f"   Topics: {enhanced_article.topics}")
    
    # Test serialization
    article_dict = enhanced_article.to_dict()
    print(f"   Serialized successfully: {len(article_dict)} fields")
    
    print("\n=== Enhanced System Test Complete ===")

if __name__ == "__main__":
    test_enhanced_system()