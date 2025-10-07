from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Any
from decimal import Decimal
import hashlib
import uuid
from datetime import timezone

@dataclass
class EnhancedArticle:
    """Industry-standard article data model"""
    
    # Core Identification
    article_id: str
    title: str
    url: str
    canonical_url: Optional[str]
    
    # Source Information
    domain: str
    source_type: str  # news, blog, social, etc.
    crawler_engine: str
    crawl_depth: int
    
    # Content
    content: str
    excerpt: Optional[str]
    summary: Optional[str]
    content_length: int
    content_hash: str
    language: str
    
    # Metadata
    authors: List[str]
    publish_date: Optional[datetime]
    last_modified: Optional[datetime]
    category: Optional[str]
    tags: List[str]
    topics: List[str]
    
    # Content Type
    content_type: str  # article, video, podcast, etc.
    schema_type: Optional[str]  # NewsArticle, BlogPosting, etc.
    structured_data: Optional[Dict[str, Any]]
    
    # Quality Metrics
    quality_score: Decimal
    confidence_score: Decimal
    sentiment: Dict[str, float]  # multi-dimensional sentiment
    spam_score: Decimal
    duplicate_of: Optional[str]
    
    # Entities & Analysis
    entities: Dict[str, List[Dict]]  # people, organizations, locations
    keywords: List[str]
    
    # Technical Info
    http_status: int
    processing_timestamp: datetime
    etag: Optional[str]
    
    # Engagement Metrics
    read_time: int  # in minutes
    social_shares: Dict[str, int]  # platform -> count
    comments_count: int
    outbound_links: List[str]
    inbound_links: List[str]
    
    # Media
    media_attachments: List[Dict[str, str]]
    amp_url: Optional[str]
    
    # Security
    security_score: Decimal
    
    def __post_init__(self):
        """Generate content hash and ensure proper types"""
        if not self.article_id:
            self.article_id = str(uuid.uuid4())
        
        if not self.content_hash:
            self.content_hash = self._generate_content_hash()
    
    def _generate_content_hash(self) -> str:
        """Generate SHA-256 hash of content for duplicate detection"""
        content_to_hash = f"{self.title}{self.content}{self.url}"
        return hashlib.sha256(content_to_hash.encode('utf-8')).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = asdict(self)
        
        # Convert datetime objects to ISO format strings
        for field in ['publish_date', 'last_modified', 'processing_timestamp']:
            if result.get(field) and isinstance(result[field], datetime):
                # Ensure consistent datetime format
                dt = result[field]
                if dt.tzinfo is not None:
                    # Convert to UTC and remove timezone for consistency
                    dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
                result[field] = dt.isoformat() + 'Z'
        
        # Convert Decimal to float for JSON
        for field in ['quality_score', 'confidence_score', 'spam_score', 'security_score']:
            if result.get(field) and isinstance(result[field], Decimal):
                result[field] = float(result[field])
        
        return result
    
    @classmethod
    def from_basic_article(cls, basic_article: Dict, enhanced_data: Dict = None) -> 'EnhancedArticle':
        """Create enhanced article from basic article data"""
        enhanced_data = enhanced_data or {}
        
        return cls(
            # Core Identification
            article_id=basic_article.get('article_id', str(uuid.uuid4())),
            title=basic_article.get('title', ''),
            url=basic_article.get('url', ''),
            canonical_url=basic_article.get('canonical_url'),
            
            # Source Information
            domain=basic_article.get('domain', ''),
            source_type=enhanced_data.get('source_type', 'news'),
            crawler_engine=basic_article.get('crawler_engine', ''),
            crawl_depth=enhanced_data.get('crawl_depth', 0),
            
            # Content
            content=basic_article.get('content', ''),
            excerpt=enhanced_data.get('excerpt'),
            summary=enhanced_data.get('summary'),
            content_length=basic_article.get('content_length', 0),
            content_hash='',  # Will be auto-generated
            language=enhanced_data.get('language', 'en'),
            
            # Metadata
            authors=basic_article.get('authors', []) or [],
            publish_date=basic_article.get('publish_date'),
            last_modified=enhanced_data.get('last_modified'),
            category=basic_article.get('category'),
            tags=enhanced_data.get('tags', []),
            topics=enhanced_data.get('topics', []),
            
            # Content Type
            content_type=enhanced_data.get('content_type', 'article'),
            schema_type=enhanced_data.get('schema_type'),
            structured_data=enhanced_data.get('structured_data'),
            
            # Quality Metrics
            quality_score=Decimal(str(basic_article.get('quality_score', 0))),
            confidence_score=Decimal(str(enhanced_data.get('confidence_score', 0.8))),
            sentiment=basic_article.get('sentiment', {}) or {},
            spam_score=Decimal(str(enhanced_data.get('spam_score', 0))),
            duplicate_of=enhanced_data.get('duplicate_of'),
            
            # Entities & Analysis
            entities=enhanced_data.get('entities', {}),
            keywords=enhanced_data.get('keywords', []),
            
            # Technical Info
            http_status=enhanced_data.get('http_status', 200),
            processing_timestamp=basic_article.get('processing_timestamp', datetime.utcnow()),
            etag=enhanced_data.get('etag'),
            
            # Engagement Metrics
            read_time=enhanced_data.get('read_time', 0),
            social_shares=enhanced_data.get('social_shares', {}),
            comments_count=enhanced_data.get('comments_count', 0),
            outbound_links=enhanced_data.get('outbound_links', []),
            inbound_links=enhanced_data.get('inbound_links', []),
            
            # Media
            media_attachments=enhanced_data.get('media_attachments', []),
            amp_url=enhanced_data.get('amp_url'),
            
            # Security
            security_score=Decimal(str(enhanced_data.get('security_score', 0.9)))
        )