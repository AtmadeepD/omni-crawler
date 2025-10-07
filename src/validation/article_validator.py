import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)

class ArticleValidator:
    """Comprehensive article data validation"""
    
    def __init__(self):
        self.quality_rules = self._initialize_quality_rules()
    
    def _initialize_quality_rules(self) -> Dict:
        """Initialize validation rules and scoring weights"""
        return {
            'content_quality': {
                'min_title_length': 10,
                'max_title_length': 200,
                'min_content_length': 50,
                'max_content_length': 50000,
                'required_fields': ['title', 'url', 'domain']
            },
            'url_validation': {
                'allowed_schemes': ['http', 'https'],
                'blocked_domains': ['spam.com', 'malicious.net']  # Extend this list
            },
            'date_validation': {
                'max_future_days': 1,
                'min_past_years': 10
            }
        }
    
    def validate_article(self, article_data: Dict) -> Tuple[bool, Dict]:
        """
        Comprehensive article validation
        Returns: (is_valid, validation_results)
        """
        validation_results = {
            'is_valid': True,
            'quality_score': Decimal('0.0'),
            'errors': [],
            'warnings': [],
            'passed_checks': []
        }
        
        # Required fields check
        if not self._check_required_fields(article_data, validation_results):
            validation_results['is_valid'] = False
        
        # URL validation
        if not self._validate_url(article_data.get('url'), validation_results):
            validation_results['is_valid'] = False
        
        # Content quality checks
        self._validate_content_quality(article_data, validation_results)
        
        # Date validation
        self._validate_dates(article_data, validation_results)
        
        # Author validation
        self._validate_authors(article_data, validation_results)
        
        # Calculate final quality score
        validation_results['quality_score'] = self._calculate_quality_score(validation_results)
        
        return validation_results['is_valid'], validation_results
    
    def _check_required_fields(self, article_data: Dict, results: Dict) -> bool:
        """Check if required fields are present"""
        required = self.quality_rules['content_quality']['required_fields']
        missing = [field for field in required if not article_data.get(field)]
        
        if missing:
            results['errors'].append(f"Missing required fields: {', '.join(missing)}")
            return False
        
        results['passed_checks'].append('required_fields')
        return True
    
    def _validate_url(self, url: Optional[str], results: Dict) -> bool:
        """Validate URL format and domain"""
        if not url:
            results['errors'].append("URL is required")
            return False
        
        try:
            parsed = urlparse(url)
            
            # Check scheme
            if parsed.scheme not in self.quality_rules['url_validation']['allowed_schemes']:
                results['errors'].append(f"Invalid URL scheme: {parsed.scheme}")
                return False
            
            # Check domain
            domain = parsed.netloc.lower()
            blocked_domains = self.quality_rules['url_validation']['blocked_domains']
            if any(blocked in domain for blocked in blocked_domains):
                results['errors'].append(f"Blocked domain: {domain}")
                return False
            
            # Check if URL looks reasonable
            if len(url) > 2000:
                results['warnings'].append("URL is unusually long")
            
            results['passed_checks'].append('url_validation')
            return True
            
        except Exception as e:
            results['errors'].append(f"URL parsing failed: {str(e)}")
            return False
    
    def _validate_content_quality(self, article_data: Dict, results: Dict):
        """Validate content quality metrics"""
        title = article_data.get('title', '')
        content = article_data.get('content', '')
        
        # Title validation
        min_title = self.quality_rules['content_quality']['min_title_length']
        max_title = self.quality_rules['content_quality']['max_title_length']
        
        if len(title) < min_title:
            results['errors'].append(f"Title too short: {len(title)} chars (min {min_title})")
        elif len(title) > max_title:
            results['warnings'].append(f"Title very long: {len(title)} chars")
        else:
            results['passed_checks'].append('title_length')
        
        # Content validation
        min_content = self.quality_rules['content_quality']['min_content_length']
        max_content = self.quality_rules['content_quality']['max_content_length']
        content_len = len(content)
        
        if content_len < min_content:
            results['warnings'].append(f"Content quite short: {content_len} chars")
        elif content_len > max_content:
            results['warnings'].append(f"Content very long: {content_len} chars")
        else:
            results['passed_checks'].append('content_length')
        
        # Check for placeholder content
        if self._looks_like_placeholder(content):
            results['warnings'].append("Content appears to be placeholder text")
    
    def _validate_dates(self, article_data: Dict, results: Dict):
        """Validate publish and modification dates"""
        publish_date = article_data.get('publish_date')
        
        if publish_date:
            try:
                # Handle both string and datetime objects
                if isinstance(publish_date, str):
                    # Parse ISO format string
                    if 'Z' in publish_date or '+' in publish_date:
                        # Offset-aware datetime (has timezone)
                        publish_date = datetime.fromisoformat(publish_date.replace('Z', '+00:00'))
                    else:
                        # Offset-naive datetime (no timezone)
                        publish_date = datetime.fromisoformat(publish_date)
                
                # Make both datetimes offset-naive for comparison
                now = datetime.utcnow()
                if publish_date.tzinfo is not None:
                    publish_date = publish_date.replace(tzinfo=None)
                
                max_future = timedelta(days=self.quality_rules['date_validation']['max_future_days'])
                max_past = timedelta(days=self.quality_rules['date_validation']['min_past_years'] * 365)
                
                if publish_date > now + max_future:
                    results['warnings'].append("Publish date is too far in the future")
                elif publish_date < now - max_past:
                    results['warnings'].append("Publish date is very old")
                else:
                    results['passed_checks'].append('date_validation')
                    
            except (ValueError, TypeError) as e:
                results['warnings'].append(f"Invalid publish date format: {publish_date}")
    
    def _validate_authors(self, article_data: Dict, results: Dict):
        """Validate author information"""
        authors = article_data.get('authors', [])
        
        if not authors:
            results['warnings'].append("No authors specified")
            return
        
        valid_authors = []
        for author in authors:
            if isinstance(author, str) and author.strip():
                # Basic author name validation
                author_clean = author.strip()
                if len(author_clean) < 2:
                    results['warnings'].append(f"Author name too short: {author}")
                elif len(author_clean) > 100:
                    results['warnings'].append(f"Author name unusually long: {author}")
                elif re.match(r'^[A-Za-z\s\.\-]+$', author_clean):
                    valid_authors.append(author_clean)
                else:
                    results['warnings'].append(f"Author name contains unusual characters: {author}")
            else:
                results['warnings'].append(f"Invalid author format: {author}")
        
        if valid_authors:
            results['passed_checks'].append('author_validation')
    
    def _looks_like_placeholder(self, content: str) -> bool:
        """Check if content appears to be placeholder text"""
        placeholder_indicators = [
            'lorem ipsum',
            'placeholder text',
            'sample content',
            'coming soon',
            'under construction'
        ]
        
        content_lower = content.lower()
        return any(indicator in content_lower for indicator in placeholder_indicators)
    
    def _calculate_quality_score(self, validation_results: Dict) -> Decimal:
        """Calculate overall quality score based on validation results"""
        total_checks = 8  # Total possible checks
        passed_checks = len(validation_results['passed_checks'])
        
        # Penalize for errors and warnings
        error_penalty = len(validation_results['errors']) * 0.3
        warning_penalty = len(validation_results['warnings']) * 0.1
        
        base_score = Decimal(str(passed_checks / total_checks))
        penalty = Decimal(str(error_penalty + warning_penalty))
        
        final_score = max(Decimal('0.0'), base_score - penalty)
        return final_score
    
    def generate_validation_report(self, article_data: Dict) -> Dict:
        """Generate comprehensive validation report"""
        is_valid, results = self.validate_article(article_data)
        
        report = {
            'validation_summary': {
                'is_valid': is_valid,
                'quality_score': float(results['quality_score']),
                'total_checks': len(results['passed_checks']) + len(results['errors']) + len(results['warnings']),
                'passed_checks': len(results['passed_checks']),
                'errors_count': len(results['errors']),
                'warnings_count': len(results['warnings'])
            },
            'detailed_results': results
        }
        
        return report