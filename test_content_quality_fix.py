import sys
import os
import re  # Added import
import requests

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from utils.content_cleaner import ContentCleaner
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_content_quality_fix():
    """Test the content cleaning on problematic articles"""
    print("🧪 Testing Content Quality Fix...")
    
    cleaner = ContentCleaner()
    
    # Test URLs - using working articles
    test_urls = [
        "https://www.bbc.com/news/world-us-canada-67890123",  # BBC news article
        "https://www.npr.org/2025/10/06/nx-s1-5560216/who-is-larry-ellison-the-billionaire-trump-friend-whos-part-of-the-tikktok-takeover",
        "https://www.bbc.com/news/articles/cvgqyj268ljo",  # BBC news article that worked
        "https://www.reuters.com/world/us/trump-biden-debate-times-rules-everything-you-need-know-2024-06-26/"  # Reuters article
    ]
    
    for url in test_urls:
        print(f"\n{'='*80}")
        print(f"🔗 Testing URL: {url}")
        print(f"{'='*80}")
        
        try:
            response = requests.get(url, timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            })
            
            if response.status_code != 200:
                print(f"❌ HTTP {response.status_code} for {url}")
                continue
                
            raw_content = response.text
            cleaned_content = cleaner.clean_html_content(raw_content, url)
            
            print(f"📊 Raw HTML length: {len(raw_content):,} chars")
            print(f"🧹 Cleaned content length: {len(cleaned_content):,} chars")
            if len(raw_content) > 0:
                reduction = ((len(raw_content) - len(cleaned_content)) / len(raw_content) * 100)
                print(f"📈 Reduction: {reduction:.1f}%")
            
            print(f"\n📖 CLEANED CONTENT (first 500 chars):")
            print(f"{cleaned_content[:500]}...")
            
            print(f"\n🔍 QUALITY CHECK:")
            print(f"   Readable: {'✅ YES' if len(cleaned_content) > 200 and ' ' in cleaned_content else '❌ NO'}")
            print(f"   Has sentences: {'✅ YES' if '. ' in cleaned_content else '❌ NO'}")
            print(f"   Word count: {len(cleaned_content.split())} words")
            if len(cleaned_content.split()) > 0:
                avg_word_len = sum(len(word) for word in cleaned_content.split()) / len(cleaned_content.split())
                print(f"   Avg word length: {avg_word_len:.1f} chars")
            
            # Check for common gibberish indicators
            gibberish_indicators = [
                'ADVERTISEMENT', 'Sign up for', 'Follow us', 'iframe',
                'script', 'style=', 'display:none', '<!--', 'Download Embed'
            ]
            
            gibberish_found = any(indicator in cleaned_content for indicator in gibberish_indicators)
            print(f"   Contains gibberish: {'❌ YES' if gibberish_found else '✅ NO'}")
            
            # Check for error pages
            error_indicators = [
                'page not found', 'sorry, we couldn\'t find that page',
                'error 404', 'not found'
            ]
            error_found = any(indicator in cleaned_content.lower() for indicator in error_indicators)
            print(f"   Is error page: {'❌ YES' if error_found else '✅ NO'}")
            
        except Exception as e:
            print(f"❌ Test failed: {e}")

def compare_old_vs_new():
    """Compare old vs new content extraction"""
    print(f"\n{'='*80}")
    print("🔄 COMPARING OLD vs NEW CONTENT EXTRACTION")
    print(f"{'='*80}")
    
    # Sample problematic HTML (simulating old extraction)
    problematic_html = """
    <div class="article">
        <script>var ad = "ADVERTISEMENT";</script>
        <style>.ad { display: none; }</style>
        <nav>Menu items here</nav>
        <header>Site header</header>
        <div class="content">
            <p>This is real article content that should be extracted.</p>
            <p>It contains meaningful sentences and paragraphs.</p>
            <div class="advertisement">Buy our product!</div>
            <p>More real content continues here.</p>
            <iframe src="https://player.npr.org"></iframe>
            <p>Download Embed Transcript</p>
        </div>
        <footer>Site footer</footer>
    </div>
    """
    
    cleaner = ContentCleaner()
    
    # Old method (basic regex)
    old_content = re.sub(r'<[^>]+>', '', problematic_html)
    old_content = re.sub(r'\s+', ' ', old_content)
    
    # New method (enhanced cleaning)
    new_content = cleaner.clean_html_content(problematic_html)
    
    print("📝 OLD METHOD (basic regex):")
    print(f"Content: {old_content[:200]}...")
    print(f"Length: {len(old_content)} chars")
    print(f"Contains 'ADVERTISEMENT': {'❌ YES' if 'ADVERTISEMENT' in old_content else '✅ NO'}")
    print(f"Contains 'iframe': {'❌ YES' if 'iframe' in old_content else '✅ NO'}")
    
    print(f"\n🆕 NEW METHOD (enhanced cleaning):")
    print(f"Content: {new_content[:200]}...")
    print(f"Length: {len(new_content)} chars")
    print(f"Contains 'ADVERTISEMENT': {'❌ YES' if 'ADVERTISEMENT' in new_content else '✅ NO'}")
    print(f"Contains 'iframe': {'❌ YES' if 'iframe' in new_content else '✅ NO'}")
    
    improvement = 'ADVERTISEMENT' not in new_content and 'ADVERTISEMENT' in old_content
    print(f"\n🎯 IMPROVEMENT: {'✅ SIGNIFICANT' if improvement else '⚠️ MINIMAL'}")

if __name__ == "__main__":
    test_content_quality_fix()
    compare_old_vs_new()