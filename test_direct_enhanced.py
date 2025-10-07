import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from storage.omni_storage import OmniStorage

def test_direct_enhanced():
    """Test enhanced storage directly"""
    print("🧪 Testing Enhanced Storage Directly...")
    
    try:
        # Initialize storage
        storage = OmniStorage()
        
        # Test article
        test_article = {
            'article_id': 'direct-test-001',
            'title': 'Direct Test - Climate Change Summit Results',
            'url': 'https://news.example.com/climate-summit-2024',
            'domain': 'news.example.com',
            'authors': ['Environmental Reporter'],
            'category': 'environment',
            'quality_score': 0.78,
            'sentiment': {'overall': 0.3},
            'content': '''
            World leaders have reached a historic agreement at the Global Climate Summit 
            to reduce carbon emissions by 50% by 2030. The agreement, signed by 195 countries, 
            represents the most ambitious climate action plan to date. 
            
            Key provisions include massive investments in renewable energy, 
            carbon capture technology development, and support for developing nations 
            transitioning to green economies. Environmental groups have praised the 
            agreement but emphasize that implementation will be critical.
            ''',
            'content_length': 450,
            'processing_timestamp': '2024-01-20T16:45:00',
            'publish_date': '2024-01-20T11:30:00',
            'crawler_engine': 'test_crawler'
        }
        
        print("💾 Testing enhanced save...")
        result = storage.save_enhanced_article(test_article)
        
        if result['success']:
            print(f"✅ Enhanced save successful!")
            print(f"   ID: {result['article_id']}")
            print(f"   Quality: {result['quality_score']:.2f}")
            print(f"   Confidence: {result['confidence_score']:.2f}")
            
            # Test retrieval
            articles = storage.get_enhanced_articles(limit=3)
            print(f"📚 Retrieved {len(articles)} enhanced articles")
            
            if articles:
                latest = articles[0]
                print(f"   Latest: {latest.get('title', 'Unknown')[:50]}...")
                print(f"   Has excerpt: {'excerpt' in latest}")
                print(f"   Has keywords: {len(latest.get('keywords', [])) > 0}")
                print(f"   Has entities: {len(latest.get('entities', {})) > 0}")
                
        else:
            print(f"❌ Enhanced save failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Direct test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_direct_enhanced()