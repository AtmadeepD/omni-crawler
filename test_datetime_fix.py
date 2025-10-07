import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from validation.article_validator import ArticleValidator

def test_datetime_fix():
    """Test the datetime validation fix"""
    print("🧪 Testing Datetime Validation Fix...")
    
    validator = ArticleValidator()
    
    # Test cases with different datetime formats
    test_cases = [
        {
            'title': 'Test with naive datetime',
            'publish_date': '2024-01-20T10:00:00',  # naive
            'url': 'https://example.com/test1',
            'domain': 'example.com'
        },
        {
            'title': 'Test with aware datetime',
            'publish_date': '2024-01-20T10:00:00Z',  # aware with Z
            'url': 'https://example.com/test2', 
            'domain': 'example.com'
        },
        {
            'title': 'Test with timezone offset',
            'publish_date': '2024-01-20T10:00:00+05:00',  # aware with offset
            'url': 'https://example.com/test3',
            'domain': 'example.com'
        }
    ]
    
    for i, test_case in enumerate(test_cases):
        print(f"\n📝 Test case {i+1}: {test_case['title']}")
        is_valid, results = validator.validate_article(test_case)
        print(f"   Valid: {is_valid}")
        print(f"   Quality Score: {results['quality_score']}")
        print(f"   Errors: {results['errors']}")
        print(f"   Warnings: {results['warnings']}")

if __name__ == "__main__":
    test_datetime_fix()