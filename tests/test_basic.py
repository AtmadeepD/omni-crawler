import asyncio
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

async def test_all_components():
    """Test all components step by step"""
    print("🚀 Starting Comprehensive Test...")
    
    try:
        # Test Discovery
        from discovery.url_discovery_engine import OmniURLDiscoverer
        print("\n1. Testing URL Discovery...")
        discoverer = OmniURLDiscoverer(redis_host='localhost')
        urls = await discoverer.run_discovery()
        print(f"   ✅ Found {len(urls)} URLs")
        
        # Test Crawler
        from crawler.multi_engine_crawler import OmniCrawlerEngine
        print("\n2. Testing Crawler Engine...")
        
        async with OmniCrawlerEngine() as crawler:
            if urls:
                test_url = urls[0]['url']
                print(f"   Testing with: {test_url}")
                result = await crawler.crawl_url(test_url)
                
                if result:
                    print(f"   ✅ Crawl successful")
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Content: {len(result['content'])} chars")
                    print(f"   Engine: {result['crawler_engine']}")
                else:
                    print("   ❌ Crawl failed")
            else:
                print("   ⚠️ No URLs to test with")
        
        print("\n🎉 All tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_all_components())