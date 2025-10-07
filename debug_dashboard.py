import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from storage.omni_storage import OmniStorage
from flask import Flask, render_template_string
import json

def debug_dashboard_issues():
    """Debug why articles aren't opening in dashboard"""
    print("🔍 Debugging Dashboard Issues...")
    
    storage = OmniStorage()
    
    # Get some articles to test
    articles = storage.get_enhanced_articles(limit=5)
    print(f"📚 Found {len(articles)} articles")
    
    for i, article in enumerate(articles, 1):
        print(f"\n{'='*60}")
        print(f"📄 Article {i}: {article.get('title', 'Unknown')}")
        print(f"{'='*60}")
        
        # Check critical fields
        print(f"🔑 Article ID: {article.get('article_id')}")
        print(f"🔗 URL: {article.get('url')}")
        print(f"📝 Title: {article.get('title')}")
        print(f"📊 Content Length: {len(article.get('content', ''))} chars")
        print(f"🌐 Domain: {article.get('domain')}")
        
        # Check if content is accessible
        content = article.get('content', '')
        if content:
            print(f"✅ Content: Available ({len(content)} chars)")
            print(f"📖 Sample: {content[:200]}...")
        else:
            print("❌ Content: MISSING")
        
        # Check enhanced fields
        enhanced_fields = [k for k in article.keys() if k not in 
                          ['article_id', 'title', 'url', 'domain', 'content']]
        print(f"✨ Enhanced Fields: {len(enhanced_fields)}")
    
    storage.close()
    
    # Test Flask template rendering
    print(f"\n{'='*60}")
    print("🧪 Testing Flask Template Rendering")
    print(f"{'='*60}")
    
    test_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Test Article</title>
    </head>
    <body>
        <h1>{{ article.title }}</h1>
        <p><strong>URL:</strong> {{ article.url }}</p>
        <p><strong>Domain:</strong> {{ article.domain }}</p>
        <div class="content">
            {{ article.content[:500] }}...
        </div>
    </body>
    </html>
    """
    
    if articles:
        test_article = articles[0]
        app = Flask(__name__)
        with app.app_context():
            try:
                rendered = render_template_string(test_template, article=test_article)
                print("✅ Flask template rendering: SUCCESS")
                print(f"📄 Rendered length: {len(rendered)} chars")
            except Exception as e:
                print(f"❌ Flask template rendering failed: {e}")

if __name__ == "__main__":
    debug_dashboard_issues()