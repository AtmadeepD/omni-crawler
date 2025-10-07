import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.storage.omni_storage import OmniStorage
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_enhanced_integration():
    """Test the complete enhanced system integration"""
    
    print("🚀 Testing Enhanced System Integration...\n")
    
    # Initialize storage
    storage = OmniStorage()
    
    # Sample article data (similar to what your crawler produces)
    sample_article = {
        'article_id': 'integration-test-001',
        'title': 'Microsoft Announces New AI Tools for Developers at Build Conference',
        'url': 'https://technews.example.com/microsoft-ai-tools-build-2024',
        'domain': 'technews.example.com',
        'authors': ['Sarah Johnson', 'Mike Chen'],
        'category': 'technology',
        'quality_score': 0.88,
        'sentiment': {'overall': 0.6},
        'content': '''
        Microsoft has unveiled a suite of new artificial intelligence tools aimed at developers 
        during its annual Build conference. The new tools include enhanced Copilot integrations, 
        improved Azure AI services, and new machine learning frameworks. CEO Satya Nadella 
        emphasized the company\'s commitment to making AI accessible to all developers. 
        
        The announcements come as competition in the AI space intensifies, with Google, 
        Amazon, and OpenAI all releasing new developer tools in recent months. Microsoft\'s 
        new offerings focus on simplifying the process of building, training, and deploying 
        AI models at scale. Early adopters have praised the improvements in development 
        workflow and reduced complexity.
        
        "These tools represent a significant step forward in democratizing AI development," 
        said Nadella during his keynote address. "We\'re removing barriers and making it 
        easier for developers of all skill levels to incorporate AI into their applications."
        ''',
        'content_length': 1200,
        'processing_timestamp': '2024-01-20T14:30:00',
        'publish_date': '2024-01-20T09:00:00',
        'crawler_engine': 'advanced_crawler_v2'
    }
    
    print("1. Testing Enhanced Article Save...")
    result = storage.save_enhanced_article(sample_article)
    
    if result['success']:
        print(f"   ✅ SUCCESS: Article {result['article_id']} saved!")
        print(f"   📊 Quality Score: {result['quality_score']}")
        print(f"   🎯 Confidence Score: {result['confidence_score']}")
        print(f"   🔧 Enhanced Fields: {len(result['enhanced_fields'])}")
    else:
        print(f"   ❌ FAILED: {result.get('error')}")
        return False
    
    print("\n2. Testing Enhanced Article Retrieval...")
    articles = storage.get_enhanced_articles(limit=5)
    print(f"   📚 Retrieved {len(articles)} articles from database")
    
    if articles:
        latest = articles[0]
        print(f"   📖 Latest Article: {latest.get('title', 'Unknown')[:60]}...")
        print(f"   🏷️  Topics: {latest.get('topics', [])}")
        print(f"   ⏱️  Read Time: {latest.get('read_time', 0)} min")
        print(f"   🔑 Keywords: {latest.get('keywords', [])[:5]}")
    
    print("\n3. Testing Enhanced Export...")
    from src.api.data_exporter import DataExporter
    exporter = DataExporter()
    
    # This would test the CSV export - we'll just check if it initializes
    print("   ✅ DataExporter ready for enhanced exports")
    
    print("\n🎉 ENHANCED SYSTEM INTEGRATION TEST COMPLETED!")
    return True

if __name__ == "__main__":
    success = test_enhanced_integration()
    sys.exit(0 if success else 1)