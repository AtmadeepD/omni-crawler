import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from storage.omni_storage import OmniStorage

def check_enhanced_articles():
    """Check the quality of enhanced articles in database"""
    print("🔍 Checking Enhanced Articles Quality...")
    
    storage = OmniStorage()
    
    # Get recent enhanced articles
    articles = storage.get_enhanced_articles(limit=10)
    print(f"📚 Found {len(articles)} enhanced articles")
    
    for i, article in enumerate(articles, 1):
        print(f"\n{'='*80}")
        print(f"📄 Article {i}: {article.get('title', 'Unknown')}")
        print(f"{'='*80}")
        print(f"🌐 Domain: {article.get('domain', 'Unknown')}")
        print(f"📊 Quality Score: {article.get('quality_score', 'N/A')}")
        print(f"🎯 Confidence: {article.get('confidence_score', 'N/A')}")
        print(f"📝 Word Count: {len(article.get('content', '').split())}")
        print(f"🔤 Language: {article.get('language', 'Unknown')}")
        
        # Check content quality
        content = article.get('content', '')
        if content:
            print(f"📖 Content Sample (first 300 chars):")
            print(f"{content[:300]}...")
            
            # Quality indicators
            has_gibberish = any(indicator in content for indicator in ['ADVERTISEMENT', 'iframe', 'script', 'style='])
            is_readable = len(content) > 200 and '. ' in content
            print(f"🔍 Quality Check:")
            print(f"   Readable: {'✅ YES' if is_readable else '❌ NO'}")
            print(f"   Contains gibberish: {'❌ YES' if has_gibberish else '✅ NO'}")
        
        # Show enhanced fields
        enhanced_fields = [k for k in article.keys() if k not in 
                          ['article_id', 'title', 'url', 'domain', 'content']]
        print(f"✨ Enhanced Fields: {len(enhanced_fields)}")
        if enhanced_fields:
            print(f"   Sample: {', '.join(enhanced_fields[:5])}...")
    
    storage.close()

if __name__ == "__main__":
    check_enhanced_articles()