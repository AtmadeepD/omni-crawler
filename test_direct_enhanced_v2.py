import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from storage.omni_storage import OmniStorage
import logging

# Enable detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_direct_enhanced():
    """Test enhanced storage directly with better error handling"""
    print("🧪 Testing Enhanced Storage Directly...")
    
    storage = None
    try:
        # Initialize storage
        print("🔄 Initializing storage...")
        storage = OmniStorage()
        print("✅ Storage initialized")
        
        # Test article
        test_article = {
            'article_id': 'direct-test-002',
            'title': 'Direct Test - Quantum Computing Breakthrough',
            'url': 'https://tech.example.com/quantum-computing-2024',
            'domain': 'tech.example.com',
            'authors': ['Science Correspondent'],
            'category': 'technology',
            'quality_score': 0.82,
            'sentiment': {'overall': 0.8},
            'content': '''
            Researchers at Google have announced a major breakthrough in quantum computing, 
            achieving quantum supremacy with their new 128-qubit processor. The system 
            solved a complex problem in 200 seconds that would take the world's fastest 
            supercomputer 10,000 years.
            
            This milestone represents a significant step forward in computational power 
            and has implications for cryptography, drug discovery, and artificial intelligence. 
            The research team emphasized that while practical applications are still years away, 
            this demonstrates the potential of quantum computing.
            ''',
            'content_length': 520,
            'processing_timestamp': '2024-01-20T17:30:00',
            'publish_date': '2024-01-20T12:15:00',
            'crawler_engine': 'test_crawler'
        }
        
        print("💾 Testing enhanced save...")
        result = storage.save_enhanced_article(test_article)
        
        if result['success']:
            print(f"✅ Enhanced save successful!")
            print(f"   ID: {result['article_id']}")
            print(f"   Quality: {result['quality_score']:.2f}")
            print(f"   Confidence: {result['confidence_score']:.2f}")
            print(f"   Enhanced Fields: {result['enhanced_fields']}")
            
            # Test retrieval
            print("\n📚 Testing article retrieval...")
            articles = storage.get_enhanced_articles(limit=5)
            print(f"   Retrieved {len(articles)} enhanced articles")
            
            if articles:
                latest = articles[0]
                print(f"   Latest Title: {latest.get('title', 'Unknown')}")
                print(f"   Has Excerpt: {'excerpt' in latest and latest['excerpt']}")
                print(f"   Has Summary: {'summary' in latest and latest['summary']}")
                print(f"   Keywords: {len(latest.get('keywords', []))}")
                print(f"   Read Time: {latest.get('read_time', 0)} min")
                print(f"   Language: {latest.get('language', 'unknown')}")
                
        else:
            print(f"❌ Enhanced save failed: {result.get('error')}")
            if 'validation_results' in result:
                print(f"   Validation Errors: {result['validation_results'].get('errors', [])}")
                print(f"   Validation Warnings: {result['validation_results'].get('warnings', [])}")
            
    except Exception as e:
        print(f"❌ Direct test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if storage:
            storage.close()
            print("🔌 Storage closed")

if __name__ == "__main__":
    test_direct_enhanced()