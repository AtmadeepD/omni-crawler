import logging
import csv
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from io import StringIO, BytesIO
import zipfile
from flask import Response
import psycopg2
from elasticsearch import Elasticsearch
from decimal import Decimal
from ..models.enhanced_article import EnhancedArticle
import io

def convert_decimals(obj):
    """
    Convert Decimal objects to float for JSON serialization
    """
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj

logger = logging.getLogger(__name__)

class DataExporter:
    def __init__(self, es_host='localhost:9200', pg_host='localhost'):
        self.es = Elasticsearch([f'http://{es_host}'])
        self.pg_config = {
            'host': pg_host,
            'database': 'news_omnidb',
            'user': 'crawler',
            'password': 'crawler123',
            'port': 5432
        }
    
    def export_articles_csv(self, filters: Dict = None) -> Response:
        """Export articles to CSV format"""
        try:
            articles = self.storage.get_enhanced_articles(limit=10000, filters=filters)
            
            if not articles:
                return self._create_error_response("No articles found matching criteria")
            
            # Create CSV in memory
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=[
                'article_id', 'title', 'url', 'domain', 'authors', 'category',
                'quality_score', 'sentiment', 'content_length', 'processing_timestamp',
                'publish_date', 'crawler_engine'
            ])
            
            writer.writeheader()
            for article in articles:
                # Convert authors list to string
                article['authors'] = ';'.join(article.get('authors', []))
                writer.writerow(article)
            
            csv_data = output.getvalue()
            output.close()
            
            filename = f"omnicrawler_articles_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            
            return Response(
                csv_data,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            return self._create_error_response(f"Export failed: {str(e)}")
    
    def export_articles_json(self, filters: Dict = None) -> Response:
        """Export articles to JSON format"""
        try:
            articles = self.storage.get_enhanced_articles(limit=10000, filters=filters)
            
            if not articles:
                return self._create_error_response("No articles found matching criteria")
            
            export_data = {
                'metadata': {
                    'exported_at': datetime.utcnow().isoformat(),
                    'total_articles': len(articles),
                    'filters_applied': filters or {}
                },
                'articles': articles
            }
            
            filename = f"omnicrawler_articles_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            
            converted_data = convert_decimals(export_data)
            return Response(
                json.dumps(converted_data, indent=2, ensure_ascii=False),
                mimetype='application/json',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            logger.error(f"JSON export failed: {e}")
            return self._create_error_response(f"Export failed: {str(e)}")
        
    def convert_decimals(self, obj):
        """
        Convert Decimal objects to float for JSON serialization
        """
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        return obj
    
    def export_analytics_report(self, days: int = 7) -> Response:
        """Export analytics report as PDF (simulated)"""
        try:
            analytics_data = self._get_analytics_data(days)
            
            # Create a comprehensive report in JSON format
            report = {
                'report_metadata': {
                    'title': 'OmniCrawler Analytics Report',
                    'period': f"Last {days} days",
                    'generated_at': datetime.utcnow().isoformat(),
                    'report_id': f"report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                },
                'summary_metrics': analytics_data.get('summary', {}),
                'category_analysis': analytics_data.get('categories', {}),
                'domain_analysis': analytics_data.get('domains', {}),
                'sentiment_analysis': analytics_data.get('sentiment', {}),
                'time_series_data': analytics_data.get('time_series', {}),
                'top_performers': analytics_data.get('top_articles', [])
            }
            
            filename = f"analytics_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            
            # Convert Decimals to floats for JSON serialization
            converted_report = self.convert_decimals(report)

            return Response(
                json.dumps(converted_report, indent=2, ensure_ascii=False),
                mimetype='application/json',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            logger.error(f"Analytics report export failed: {e}")
            return self._create_error_response(f"Report generation failed: {str(e)}")
    
    def export_full_database_dump(self) -> Response:
        """Export complete database dump (articles + entities)"""
        try:
            # Create in-memory zip file
            zip_buffer = BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Export articles
                articles = self._get_articles_for_export()
                articles_json = json.dumps(articles, indent=2, ensure_ascii=False)
                zip_file.writestr('articles.json', articles_json)
                
                # Export entities
                entities = self._get_entities_for_export()
                entities_json = json.dumps(entities, indent=2, ensure_ascii=False)
                zip_file.writestr('entities.json', entities_json)
                
                # Export metadata
                metadata = {
                    'exported_at': datetime.utcnow().isoformat(),
                    'total_articles': len(articles),
                    'total_entities': len(entities),
                    'database_schema': self._get_database_schema()
                }
                metadata_json = json.dumps(metadata, indent=2, ensure_ascii=False)
                zip_file.writestr('metadata.json', metadata_json)
            
            zip_buffer.seek(0)
            filename = f"omnicrawler_full_dump_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
            
            return Response(
                zip_buffer.getvalue(),
                mimetype='application/zip',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            logger.error(f"Full database dump failed: {e}")
            return self._create_error_response(f"Dump failed: {str(e)}")
    
    def export_enhanced_articles_csv(self, filters: Dict = None) -> Response:
        """Export articles with enhanced fields"""
        try:
            articles = self.storage.get_enhanced_articles(limit=10000, filters=filters)
            
            # Convert to enhanced format for export
            enhanced_data = []
            for article in articles:
                enhanced_article = EnhancedArticle.from_basic_article(article)
                enhanced_data.append(enhanced_article.to_dict())
            
            # Create CSV with enhanced fields
            csv_output = self._convert_to_enhanced_csv(enhanced_data)
            
            filename = f"enhanced_articles_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            
            return Response(
                csv_output,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename={filename}'}
            )
            
        except Exception as e:
            logger.error(f"Enhanced CSV export failed: {e}")
            return self._create_error_response(f"Export failed: {str(e)}")

    def _convert_to_enhanced_csv(self, articles: List[Dict]) -> str:
        """Convert enhanced articles to CSV format"""
        if not articles:
            return "No articles found"
        
        # Define the enhanced CSV columns
        fieldnames = [
            'article_id', 'title', 'url', 'domain', 'authors', 'category',
            'quality_score', 'sentiment', 'content_length', 'processing_timestamp',
            'publish_date', 'crawler_engine', 'summary', 'excerpt', 'keywords',
            'entities', 'language', 'read_time', 'topics', 'confidence_score'
        ]
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        
        for article in articles:
            # Flatten some nested structures for CSV
            row = {field: article.get(field, '') for field in fieldnames}
            
            # Handle nested structures
            row['authors'] = '; '.join(article.get('authors', []))
            row['keywords'] = '; '.join(article.get('keywords', []))
            row['topics'] = '; '.join(article.get('topics', []))
            row['sentiment'] = str(article.get('sentiment', {}).get('overall', 0))
            
            writer.writerow(row)
        
        return output.getvalue()
    
    def _get_articles_for_export(self, filters: Dict = None) -> List[Dict]:
        """Get articles for export with optional filtering and column checking"""
        conn = psycopg2.connect(**self.pg_config)
        cur = conn.cursor()
        
        try:
            # First, check which columns actually exist
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='articles'
            """)
            existing_columns = {row[0] for row in cur.fetchall()}
            
            # Build query with only existing columns
            base_columns = [
                'article_id', 'title', 'url', 'domain', 'authors',
                'quality_score', 'category', 'processing_timestamp'
            ]
            
            # Add optional columns if they exist
            optional_columns = [
                ('sentiment_label', 'sentiment'),
                ('publish_date', 'publish_date'),
                ('crawler_engine', 'crawler_engine')
            ]
            
            selected_columns = base_columns.copy()
            for db_column, export_field in optional_columns:
                if db_column in existing_columns:
                    selected_columns.append(db_column)
            
            # Add content length calculation
            if 'content' in existing_columns:
                selected_columns.append('LENGTH(content) as content_length')
            else:
                # If content column doesn't exist, use content_length if it exists
                if 'content_length' in existing_columns:
                    selected_columns.append('content_length')
                else:
                    selected_columns.append('0 as content_length')
            
            # Build the query
            query = f"""
                SELECT {', '.join(selected_columns)}
                FROM articles 
                WHERE 1=1
            """
            params = []
            
            if filters:
                if filters.get('domain'):
                    query += " AND domain = %s"
                    params.append(filters['domain'])
                
                if filters.get('category'):
                    query += " AND category = %s"
                    params.append(filters['category'])
                
                if filters.get('date_from'):
                    query += " AND processing_timestamp >= %s"
                    params.append(filters['date_from'])
                
                if filters.get('date_to'):
                    query += " AND processing_timestamp <= %s"
                    params.append(filters['date_to'])
                
                if filters.get('min_quality'):
                    query += " AND quality_score >= %s"
                    params.append(filters['min_quality'])
            
            query += " ORDER BY processing_timestamp DESC"
            
            if filters and filters.get('limit'):
                query += " LIMIT %s"
                params.append(filters['limit'])
            else:
                query += " LIMIT 10000"  # Safety limit
            
            cur.execute(query, params)
            
            articles = []
            for row in cur.fetchall():
                article = {
                    'article_id': row[0],
                    'title': row[1],
                    'url': row[2],
                    'domain': row[3],
                    'authors': row[4] or [],
                    'quality_score': row[5],
                    'category': row[6],
                    'processing_timestamp': row[7].isoformat() if row[7] else None
                }
                
                # Map remaining columns dynamically
                index = 8  # Start after base columns
                
                # sentiment_label
                if 'sentiment_label' in existing_columns and index < len(row):
                    article['sentiment'] = row[index]
                    index += 1
                else:
                    article['sentiment'] = 'neutral'
                
                # publish_date
                if 'publish_date' in existing_columns and index < len(row):
                    article['publish_date'] = row[index].isoformat() if row[index] else None
                    index += 1
                else:
                    article['publish_date'] = None
                
                # crawler_engine
                if 'crawler_engine' in existing_columns and index < len(row):
                    article['crawler_engine'] = row[index]
                    index += 1
                else:
                    article['crawler_engine'] = 'simple_crawler'
                
                # content_length
                if index < len(row):
                    article['content_length'] = row[index] or 0
                else:
                    article['content_length'] = 0
                
                articles.append(article)
            
            return articles
            
        finally:
            cur.close()
            conn.close()
    
    def _get_entities_for_export(self) -> List[Dict]:
        """Get entities for export"""
        conn = psycopg2.connect(**self.pg_config)
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT ae.article_id, ae.entity_type, ae.entity_name, a.title, a.domain
                FROM article_entities ae
                JOIN articles a ON ae.article_id = a.article_id
                ORDER BY ae.entity_type, ae.entity_name
            """)
            
            entities = []
            for row in cur.fetchall():
                entities.append({
                    'article_id': row[0],
                    'entity_type': row[1],
                    'entity_name': row[2],
                    'article_title': row[3],
                    'domain': row[4]
                })
            
            return entities
            
        finally:
            cur.close()
            conn.close()
    
    def _get_analytics_data(self, days: int) -> Dict:
        """Get analytics data for report generation"""
        conn = psycopg2.connect(**self.pg_config)
        cur = conn.cursor()
        
        try:
            # Summary metrics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_articles,
                    AVG(quality_score) as avg_quality,
                    COUNT(DISTINCT domain) as unique_domains,
                    COUNT(DISTINCT category) as unique_categories
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '%s days'
            """, (days,))
            
            summary_row = cur.fetchone()
            summary = {
                'total_articles': summary_row[0],
                'average_quality': round(summary_row[1] or 0, 2),
                'unique_domains': summary_row[2],
                'unique_categories': summary_row[3]
            }
            
            # Category analysis
            cur.execute("""
                SELECT category, COUNT(*) as count, AVG(quality_score) as avg_quality
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '%s days'
                GROUP BY category 
                ORDER BY count DESC
            """, (days,))
            
            categories = {}
            for row in cur.fetchall():
                categories[row[0] or 'unknown'] = {
                    'count': row[1],
                    'avg_quality': round(row[2] or 0, 2)
                }
            
            # Domain analysis
            cur.execute("""
                SELECT domain, COUNT(*) as count, AVG(quality_score) as avg_quality
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '%s days'
                GROUP BY domain 
                ORDER BY count DESC
                LIMIT 20
            """, (days,))
            
            domains = {}
            for row in cur.fetchall():
                domains[row[0]] = {
                    'count': row[1],
                    'avg_quality': round(row[2] or 0, 2)
                }
            
            # Time series data (articles per day)
            cur.execute("""
                SELECT DATE(processing_timestamp) as date, COUNT(*) as count
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '%s days'
                GROUP BY DATE(processing_timestamp)
                ORDER BY date
            """, (days,))
            
            time_series = []
            for row in cur.fetchall():
                time_series.append({
                    'date': row[0].isoformat(),
                    'article_count': row[1]
                })
            
            # Top articles by quality
            cur.execute("""
                SELECT article_id, title, domain, quality_score, processing_timestamp
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY quality_score DESC
                LIMIT 10
            """, (days,))
            
            top_articles = []
            for row in cur.fetchall():
                top_articles.append({
                    'article_id': row[0],
                    'title': row[1],
                    'domain': row[2],
                    'quality_score': row[3],
                    'processed_at': row[4].isoformat() if row[4] else None
                })
            
            return {
                'summary': summary,
                'categories': categories,
                'domains': domains,
                'time_series': time_series,
                'top_articles': top_articles
            }
            
        finally:
            cur.close()
            conn.close()
    
    def _get_database_schema(self) -> Dict:
        """Get database schema information"""
        conn = psycopg2.connect(**self.pg_config)
        cur = conn.cursor()
        
        try:
            # Get table schemas
            cur.execute("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
            """)
            
            schema = {}
            for row in cur.fetchall():
                table_name = row[0]
                if table_name not in schema:
                    schema[table_name] = []
                
                schema[table_name].append({
                    'column_name': row[1],
                    'data_type': row[2],
                    'is_nullable': row[3]
                })
            
            return schema
            
        finally:
            cur.close()
            conn.close()
    
    def _create_error_response(self, message: str) -> Response:
        """Create error response"""
        return Response(
            json.dumps({'error': message}),
            mimetype='application/json',
            status=400
        )