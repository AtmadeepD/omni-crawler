import asyncio
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from orchestrator.omni_orchestrator import OmniOrchestrator

async def test_enhanced_save():
    """Test if the enhanced save system is working"""
    print("🧪 Testing Enhanced Save in Orchestrator...")
    
    try:
        # Create orchestrator with minimal config
        orchestrator = OmniOrchestrator()
        
        # Create a test article
        test_article = {
            'article_id': 'test-enhanced-001',
            'title': 'Test Enhanced Article - AI Breakthrough in Healthcare',
            'url': 'https://test.example.com/ai-healthcare-breakthrough',
            'domain': 'test.example.com',
            'authors': ['Dr. Sarah Wilson', 'Prof. James Chen'],
            'category': 'technology',
            'quality_score': 0.85,
            'sentiment': {'overall': 0.7},
            'content': '''
            Researchers at Stanford University have developed a new AI system that can 
            diagnose rare diseases with 95% accuracy. The system, called MediAI, uses 
            deep learning to analyze medical images and patient data. This breakthrough 
            could revolutionize healthcare in remote areas where specialist doctors are scarce.
            
            "This technology has the potential to save countless lives," said Dr. Wilson, 
            lead researcher on the project. "We're particularly excited about its 
            applications in developing countries."
            
            The team plans to conduct clinical trials next year and hopes to deploy 
            the system in hospitals within three years. Other institutions including 
            MIT and Johns Hopkins are also developing similar AI diagnostic tools.
            ''',
            'content_length': 650,
            'processing_timestamp': '2024-01-20T15:30:00',
            'publish_date': '2024-01-20T10:00:00',
            'crawler_engine': 'test_crawler'
        }
        
        print("📝 Testing enhanced article save...")
        result = orchestrator.storage.save_enhanced_article(test_article)
        
        if result['success']:
            print(f"✅ SUCCESS: Enhanced article saved!")
            print(f"   Article ID: {result['article_id']}")
            print(f"   Quality Score: {result['quality_score']:.2f}")
            print(f"   Confidence: {result['confidence_score']:.2f}")
            print(f"   Enhanced Fields: {len(result['enhanced_fields'])}")
            
            # Test retrieval
            print("\n📖 Testing article retrieval...")
            articles = orchestrator.storage.get_enhanced_articles(limit=5)
            print(f"   Retrieved {len(articles)} enhanced articles from database")
            
        else:
            print(f"❌ FAILED: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_enhanced_save())