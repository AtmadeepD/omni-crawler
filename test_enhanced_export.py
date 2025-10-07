import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from storage.omni_storage import OmniStorage

def test_enhanced_export():
    """Test enhanced article export with all fields"""
    print("🧪 Testing Enhanced Article Export...")
    
    storage = OmniStorage()
    
    # Get enhanced articles
    articles = storage.get_enhanced_articles(limit=10)
    print(f"📚 Retrieved {len(articles)} enhanced articles")
    
    if articles:
        # Show sample of enhanced data
        sample = articles[0]
        print(f"\n📖 Sample Enhanced Article:")
        print(f"   Title: {sample.get('title', 'Unknown')}")
        print(f"   Domain: {sample.get('domain', 'Unknown')}")
        print(f"   Quality Score: {sample.get('quality_score', 0)}")
        print(f"   Confidence: {sample.get('confidence_score', 0)}")
        print(f"   Excerpt: {sample.get('excerpt', '')[:100]}...")
        print(f"   Summary: {sample.get('summary', '')[:100]}...")
        print(f"   Keywords: {sample.get('keywords', [])[:5]}")
        print(f"   Read Time: {sample.get('read_time', 0)} min")
        print(f"   Language: {sample.get('language', 'unknown')}")
        print(f"   Topics: {sample.get('topics', [])}")
        
        # Show enhanced fields count
        enhanced_fields = [k for k in sample.keys() if k not in ['article_id', 'title', 'url', 'domain', 'authors', 'category', 'quality_score', 'sentiment', 'content_length', 'processing_timestamp', 'publish_date', 'crawler_engine']]
        print(f"   Enhanced Fields: {len(enhanced_fields)} additional fields")
        
    storage.close()

if __name__ == "__main__":
    test_enhanced_export()