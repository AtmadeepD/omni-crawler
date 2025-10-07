import logging
import sys
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, send_file
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import psycopg2
from elasticsearch import Elasticsearch
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import json
import os
import yaml
from prometheus_client import generate_latest, Counter, Histogram, Gauge
from src.search.advanced_search import AdvancedSearchEngine
from src.monitoring.system_monitor import SystemMonitor
from src.alerts.alert_engine import AlertEngine
from src.api.data_exporter import DataExporter
from typing import Dict
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('web_dashboard.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Metrics
REQUEST_COUNT = Counter('web_requests_total', 'Total web requests', ['method', 'endpoint', 'status'])
REQUEST_LATENCY = Histogram('web_request_latency_seconds', 'Request latency')
ACTIVE_USERS = Gauge('web_active_users', 'Active users')
ARTICLE_COUNT = Gauge('web_article_count', 'Total articles in database')

class WebDashboard:
    def __init__(self, config_path='config/web_config.yaml'):
        self.app = Flask(__name__)
        self.config = self._load_config(config_path)
        self._setup_app()
        self._setup_routes()
        self._setup_metrics()
        
        # Check database schema on startup
        self._check_database_schema()

        # Initialize components
        self.search_engine = AdvancedSearchEngine()
        self.system_monitor = SystemMonitor()
        self.system_monitor.start_monitoring(interval=60)
        self.alert_engine = AlertEngine()
        self.data_exporter = DataExporter()

    def _load_config(self, config_path):
        """Load configuration with fallbacks"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Config load failed, using defaults: {e}")
            return {
                'web': {'host': '0.0.0.0', 'port': 5000, 'debug': False},
                'api': {'rate_limit': '100 per minute'},
                'security': {'enable_rate_limiting': True}
            }
    
    def _setup_app(self):
        """Configure Flask application"""
        self.app.secret_key = self.config['web'].get('secret_key', 'dev-secret-key')
        
        # Security middleware
        if self.config['security'].get('enable_cors', True):
            CORS(self.app, origins=self.config['security'].get('allowed_origins', ['*']))
        
        # Rate limiting
        if self.config['security'].get('enable_rate_limiting', True):
            self.limiter = Limiter(
                get_remote_address,
                app=self.app,
                default_limits=[self.config['api']['rate_limit']],
                storage_uri="memory://",
            )
        
        # Template and static folders
        self.app.template_folder = os.path.join(os.path.dirname(__file__), 'templates')
        self.app.static_folder = os.path.join(os.path.dirname(__file__), 'static')
    
    def _setup_metrics(self):
        """Setup Prometheus metrics endpoint"""
        @self.app.route('/metrics')
        def metrics():
            return generate_latest()
    
    def _setup_routes(self):
        """Setup all application routes with enhanced article functionality"""
        
        @self.app.before_request
        def before_request():
            request.start_time = time.time()
        
        @self.app.after_request
        def after_request(response):
            latency = time.time() - request.start_time
            REQUEST_LATENCY.observe(latency)
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=request.endpoint or 'unknown',
                status=response.status_code
            ).inc()
            
            # Security headers
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            
            return response
        
        # ===== MAIN PAGES =====
        @self.app.route('/')
        def dashboard():
            ACTIVE_USERS.inc()
            return render_template('dashboard.html')
        
        @self.app.route('/analytics')
        def analytics():
            return render_template('analytics.html')
        
        @self.app.route('/monitoring')
        def monitoring_page():
            return render_template('monitoring.html')
        
        @self.app.route('/search')
        def search_page():
            return render_template('search.html')
        
        @self.app.route('/alerts')
        def alerts_page():
            return render_template('alerts.html')

        @self.app.route('/export')
        def export_page():
            return render_template('export.html')
        
        # ===== NEW: ARTICLE DETAIL PAGE =====
        @self.app.route('/article/<article_id>')
        def article_detail_page(article_id):
            """Article detail page - Renders the full article view"""
            return render_template('article_detail.html', article_id=article_id)
        
        # ===== API ENDPOINTS =====
        @self.app.route('/api/health')
        def health_check():
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat(),
                'version': '1.0.0',
                'services': self._check_services()
            }
            return jsonify(health_status)
        
        # ===== NEW: COMPREHENSIVE ARTICLE DETAIL ENDPOINT =====
        @self.app.route('/api/articles/<article_id>')
        def get_article_detail(article_id):
            """Get detailed article information"""
            try:
                logger.info(f"Fetching article details for ID: {article_id}")
                
                conn = self._get_db_connection()
                cur = conn.cursor()
                
                # First, let's see what columns we actually have
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'articles'
                """)
                available_columns = [row[0] for row in cur.fetchall()]
                logger.info(f"Available columns: {available_columns}")
                
                # Build the query with only columns that exist
                select_columns = []
                if 'article_id' in available_columns:
                    select_columns.append('article_id')
                if 'title' in available_columns:
                    select_columns.append('title')
                if 'url' in available_columns:
                    select_columns.append('url')
                if 'domain' in available_columns:
                    select_columns.append('domain')
                if 'authors' in available_columns:
                    select_columns.append('authors')
                if 'quality_score' in available_columns:
                    select_columns.append('quality_score')
                if 'category' in available_columns:
                    select_columns.append('category')
                if 'sentiment_label' in available_columns:
                    select_columns.append('sentiment_label')
                if 'processing_timestamp' in available_columns:
                    select_columns.append('processing_timestamp')
                if 'content_length' in available_columns:
                    select_columns.append('content_length')
                if 'content' in available_columns:
                    select_columns.append('content')
                
                if not select_columns:
                    return jsonify({'error': 'No valid columns found in articles table'}), 500
                
                columns_str = ', '.join(select_columns)
                query = f"SELECT {columns_str} FROM articles WHERE article_id = %s"
                
                logger.info(f"Executing: {query}")
                cur.execute(query, (article_id,))
                
                row = cur.fetchone()
                
                if not row:
                    return jsonify({'error': 'Article not found'}), 404
                
                # Build the article object
                article = {'id': article_id}
                for i, col in enumerate(select_columns):
                    value = row[i]
                    
                    # Handle different data types
                    if isinstance(value, datetime):
                        article[col] = value.isoformat() if value else None
                    elif col == 'authors' and value and isinstance(value, list):
                        article[col] = value
                    elif col == 'authors' and value and isinstance(value, str):
                        # Try to parse string as array
                        try:
                            article[col] = json.loads(value)
                        except:
                            article[col] = [value]
                    else:
                        article[col] = value
                
                # Map column names to expected frontend names
                if 'sentiment_label' in article:
                    article['sentiment'] = article.pop('sentiment_label')
                if 'processing_timestamp' in article:
                    article['processed_at'] = article.pop('processing_timestamp')
                
                # Set defaults for missing fields
                if 'content' not in article:
                    article['content'] = 'No content available'
                if 'quality_score' not in article:
                    article['quality_score'] = 0
                if 'sentiment' not in article:
                    article['sentiment'] = 'neutral'
                if 'reading_time' not in article:
                    article['reading_time'] = 5
                
                logger.info(f"Successfully loaded article: {article_id}")
                
                cur.close()
                conn.close()
                
                return jsonify(article)
                
            except Exception as e:
                logger.error(f"Article detail error for {article_id}: {str(e)}", exc_info=True)
                return jsonify({'error': f'Database error: {str(e)}'}), 500
        
        @self.app.route('/api/stats/overview')
        def stats_overview():
            try:
                stats = self._get_system_stats()
                return jsonify(stats)
            except Exception as e:
                logger.error(f"Stats overview error: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/articles')
        def get_articles():
            try:
                page = request.args.get('page', 1, type=int)
                per_page = min(request.args.get('per_page', 20, type=int), 100)
                domain = request.args.get('domain', '')
                category = request.args.get('category', '')
                min_quality = request.args.get('min_quality', 0, type=int)
                
                articles, total = self._get_articles_paginated(
                    page, per_page, domain, category, min_quality
                )
                
                return jsonify({
                    'articles': articles,
                    'pagination': {
                        'page': page,
                        'per_page': per_page,
                        'total': total,
                        'pages': (total + per_page - 1) // per_page
                    }
                })
            except Exception as e:
                logger.error(f"Get articles error: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/articles/search')
        def search_articles():
            try:
                query = request.args.get('q', '')
                size = min(request.args.get('size', 20, type=int), 50)
                
                if not query:
                    return jsonify({'articles': [], 'total': 0})
                
                articles = self._search_articles(query, size)
                return jsonify({
                    'articles': articles,
                    'total': len(articles),
                    'query': query
                })
            except Exception as e:
                logger.error(f"Search articles error: {e}")
                return jsonify({'error': 'Search service unavailable'}), 503
        
        # ===== EXISTING ANALYTICS ENDPOINTS =====
        @self.app.route('/api/analytics/categories')
        def analytics_categories():
            try:
                data = self._get_category_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Category analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        @self.app.route('/api/analytics/sentiment')
        def analytics_sentiment():
            try:
                data = self._get_sentiment_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Sentiment analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        @self.app.route('/api/analytics/domains')
        def analytics_domains():
            try:
                data = self._get_domain_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Domain analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        # ===== EXISTING ADVANCED FEATURES =====
        @self.app.route('/api/search/advanced')
        def advanced_search():
            try:
                search_params = {
                    'query': request.args.get('q', ''),
                    'domains': request.args.getlist('domains'),
                    'categories': request.args.getlist('categories'),
                    'date_from': request.args.get('date_from'),
                    'date_to': request.args.get('date_to'),
                    'min_quality': request.args.get('min_quality', 0, type=int),
                    'sentiment': request.args.get('sentiment'),
                    'min_length': request.args.get('min_length', 0, type=int),
                    'size': request.args.get('size', 20, type=int),
                    'from': request.args.get('from', 0, type=int),
                    'sort_by': request.args.get('sort_by', 'processing_timestamp'),
                    'sort_order': request.args.get('sort_order', 'desc'),
                    'exact_match': request.args.get('exact_match', 'false').lower() == 'true'
                }
                
                results = self.search_engine.search_articles(search_params)
                return jsonify(results)
                
            except Exception as e:
                logger.error(f"Advanced search error: {e}")
                return jsonify({'error': 'Search failed', 'details': str(e)}), 500

        @self.app.route('/api/monitoring/health')
        def monitoring_health():
            try:
                health = self.system_monitor.get_health_status()
                return jsonify(health)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({'status': 'unknown', 'error': str(e)}), 500

        @self.app.route('/api/monitoring/metrics')
        def monitoring_metrics():
            try:
                metrics = self.system_monitor.get_current_metrics()
                return jsonify(metrics)
            except Exception as e:
                logger.error(f"Metrics fetch error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/monitoring/alerts')
        def monitoring_alerts():
            try:
                alerts = []
                alert_data = self.system_monitor.redis.lrange('alerts:recent', 0, 49)
                
                for alert_json in alert_data:
                    try:
                        alerts.append(json.loads(alert_json))
                    except:
                        pass
                        
                return jsonify({'alerts': alerts})
            except Exception as e:
                logger.error(f"Alerts fetch error: {e}")
                return jsonify({'alerts': []})
            

        @self.app.route('/api/alerts')
        def get_alerts():
            try:
                limit = request.args.get('limit', 50, type=int)
                alerts = self.alert_engine.get_recent_alerts(limit)
                return jsonify({'alerts': alerts})
            except Exception as e:
                logger.error(f"Get alerts error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/alerts/stats')
        def get_alert_stats():
            try:
                stats = self.alert_engine.get_alert_stats()
                return jsonify(stats)
            except Exception as e:
                logger.error(f"Get alert stats error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/alerts/<alert_id>/acknowledge', methods=['POST'])
        def acknowledge_alert(alert_id):
            try:
                self.alert_engine.acknowledge_alert(alert_id)
                return jsonify({'status': 'acknowledged'})
            except Exception as e:
                logger.error(f"Acknowledge alert error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/export/articles/csv')
        def export_articles_csv():
            try:
                filters = {
                    'domain': request.args.get('domain'),
                    'category': request.args.get('category'),
                    'date_from': request.args.get('date_from'),
                    'date_to': request.args.get('date_to'),
                    'min_quality': request.args.get('min_quality', type=int),
                    'limit': request.args.get('limit', 10000, type=int)
                }
                return self.data_exporter.export_articles_csv(filters)
            except Exception as e:
                logger.error(f"CSV export error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/export/articles/json')
        def export_articles_json():
            try:
                filters = {
                    'domain': request.args.get('domain'),
                    'category': request.args.get('category'),
                    'date_from': request.args.get('date_from'),
                    'date_to': request.args.get('date_to'),
                    'min_quality': request.args.get('min_quality', type=int),
                    'limit': request.args.get('limit', 10000, type=int)
                }
                return self.data_exporter.export_articles_json(filters)
            except Exception as e:
                logger.error(f"JSON export error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.errorhandler(404)
        def not_found(error):
            return jsonify({'error': 'Endpoint not found'}), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            logger.error(f"Internal server error: {error}")
            return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.errorhandler(429)
        def ratelimit_handler(e):
            return jsonify({'error': 'Rate limit exceeded'}), 429
        
        # ===== DEBUG ENDPOINTS =====
        @self.app.route('/api/debug/schema')
        def debug_schema():
            """Debug endpoint to check database schema"""
            try:
                conn = self._get_db_connection()
                cur = conn.cursor()
                
                # Get all columns from articles table
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = 'articles'
                    ORDER BY ordinal_position
                """)
                columns = cur.fetchall()
                
                # Get sample article to see data structure
                cur.execute("SELECT article_id, title FROM articles LIMIT 1")
                sample = cur.fetchone()
                
                cur.close()
                conn.close()
                
                return jsonify({
                    'columns': [{'name': col[0], 'type': col[1], 'nullable': col[2]} for col in columns],
                    'sample_article': {'id': sample[0], 'title': sample[1]} if sample else None
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/debug/articles')
        def debug_articles():
            """Get list of available article IDs for testing"""
            try:
                conn = self._get_db_connection()
                cur = conn.cursor()
                
                cur.execute("SELECT article_id, title FROM articles LIMIT 10")
                articles = cur.fetchall()
                
                cur.close()
                conn.close()
                
                return jsonify({
                    'articles': [{'id': row[0], 'title': row[1]} for row in articles]
                })
                
            except Exception as e:
                return jsonify({'error': str(e)}), 500
    
    def _get_db_connection(self):
        """Get PostgreSQL connection with error handling"""
        try:
            return psycopg2.connect(
                host="localhost",
                database="news_omnidb",
                user="crawler",
                password="crawler123",
                port=5432
            )
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _get_es_connection(self):
        """Get Elasticsearch connection with error handling"""
        try:
            es = Elasticsearch(['http://localhost:9200'])
            if not es.ping():
                raise Exception("Elasticsearch not available")
            return es
        except Exception as e:
            logger.error(f"Elasticsearch connection failed: {e}")
            raise
    
    def _check_services(self):
        """Check health of all dependent services"""
        services = {}
        
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            services['postgresql'] = 'healthy'
            conn.close()
        except Exception as e:
            services['postgresql'] = f'unhealthy: {str(e)}'
        
        try:
            es = self._get_es_connection()
            services['elasticsearch'] = 'healthy'
        except Exception as e:
            services['elasticsearch'] = f'unhealthy: {str(e)}'
        
        services['redis'] = 'healthy'
        
        return services
    
    def _get_system_stats(self):
        """Get comprehensive system statistics"""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        stats = {}
        
        cur.execute("SELECT COUNT(*) FROM articles")
        stats['total_articles'] = cur.fetchone()[0]
        ARTICLE_COUNT.set(stats['total_articles'])
        
        cur.execute("SELECT COUNT(DISTINCT domain) FROM articles")
        stats['unique_domains'] = cur.fetchone()[0]
        
        cur.execute("""
            SELECT 
                AVG(quality_score) as avg_quality,
                MIN(quality_score) as min_quality,
                MAX(quality_score) as max_quality,
                COUNT(CASE WHEN quality_score >= 80 THEN 1 END) as high_quality_count
            FROM articles
        """)
        quality_stats = cur.fetchone()
        stats['quality'] = {
            'average': round(quality_stats[0] or 0, 1),
            'min': quality_stats[1] or 0,
            'max': quality_stats[2] or 0,
            'high_quality_percentage': round((quality_stats[3] / stats['total_articles'] * 100), 1) if stats['total_articles'] > 0 else 0
        }
        
        cur.execute("""
            SELECT COUNT(*) FROM articles 
            WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        stats['recent_articles_1h'] = cur.fetchone()[0]
        
        cur.execute("""
            SELECT category, COUNT(*) as count 
            FROM articles 
            GROUP BY category 
            ORDER BY count DESC
        """)
        stats['categories'] = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.close()
        conn.close()
        
        return stats
    
    def _get_articles_paginated(self, page, per_page, domain, category, min_quality):
        """Get paginated articles with filtering - ENHANCED for clickable articles"""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        conditions = ["1=1"]
        params = []
        
        if domain:
            conditions.append("domain = %s")
            params.append(domain)
        
        if category:
            conditions.append("category = %s")
            params.append(category)
        
        if min_quality > 0:
            conditions.append("quality_score >= %s")
            params.append(min_quality)
        
        where_clause = " AND ".join(conditions)
        
        count_query = f"SELECT COUNT(*) FROM articles WHERE {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
        
        offset = (page - 1) * per_page
        query = f"""
            SELECT 
                article_id, title, url, domain, authors, 
                quality_score, category, sentiment_label,
                processing_timestamp, content_length, summary
            FROM articles 
            WHERE {where_clause}
            ORDER BY processing_timestamp DESC 
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, offset])
        
        cur.execute(query, params)
        
        articles = []
        for row in cur.fetchall():
            articles.append({
                'id': row[0],
                'title': row[1],
                'url': row[2],
                'domain': row[3],
                'authors': row[4] or [],
                'quality_score': row[5],
                'category': row[6],
                'sentiment': row[7],
                'processed_at': row[8].isoformat() if row[8] else None,
                'content_length': row[9] or 0,
                'summary': row[10] or '',
                # Add preview for listings
                'preview': (row[10] or '')[:150] + '...' if row[10] and len(row[10]) > 150 else (row[10] or 'No summary available')
            })
        
        cur.close()
        conn.close()
        
        return articles, total
    
    def _search_articles(self, query, size):
        """Search articles using Elasticsearch - ENHANCED for clickable articles"""
        es = self._get_es_connection()
        
        response = es.search(
            index='news-articles',
            body={
                'query': {
                    'multi_match': {
                        'query': query,
                        'fields': ['title^3', 'content^2', 'key_phrases^2', 'authors'],
                        'fuzziness': 'AUTO'
                    }
                },
                'size': size,
                'sort': [
                    {'_score': {'order': 'desc'}},
                    {'processing_timestamp': {'order': 'desc'}}
                ],
                'highlight': {
                    'fields': {
                        'title': {},
                        'content': {'fragment_size': 150, 'number_of_fragments': 1},
                        'key_phrases': {}
                    }
                }
            }
        )
        
        articles = []
        for hit in response['hits']['hits']:
            source = hit['_source']
            highlight = hit.get('highlight', {})
            
            articles.append({
                'id': hit['_id'],
                'title': source.get('title', ''),
                'url': source.get('url', ''),
                'domain': source.get('domain', ''),
                'quality_score': source.get('quality_score', 0),
                'category': source.get('category', 'general'),
                'sentiment': source.get('sentiment', {}).get('label', 'neutral'),
                'content_preview': source.get('content', '')[:200] + '...',
                'summary': source.get('summary', ''),
                'score': hit['_score'],
                'highlight': highlight,
                # Add preview for listings
                'preview': source.get('summary', source.get('content', '')[:150] + '...')
            })
        
        return articles
    
    def _get_category_analytics(self):
        """Get category distribution analytics"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT category, COUNT(*) as count, AVG(quality_score) as avg_quality
                FROM articles 
                GROUP BY category 
                ORDER BY count DESC
            """)
            
            categories = []
            counts = []
            avg_qualities = []
            
            for row in cur.fetchall():
                category_name = row[0] or 'unknown'
                categories.append(category_name)
                counts.append(row[1])
                avg_qualities.append(float(round(row[2] or 0, 1)))
            
            cur.close()
            conn.close()
            
            if not categories:
                fig = go.Figure()
                fig.update_layout(
                    title='No Category Data Available',
                    xaxis_title='Category',
                    yaxis_title='Article Count',
                    annotations=[{
                        'text': 'No articles found in database',
                        'xref': 'paper', 'yref': 'paper',
                        'showarrow': False, 'font': {'size': 16}
                    }]
                )
            else:
                fig = go.Figure(data=[
                    go.Bar(name='Article Count', x=categories, y=counts, yaxis='y'),
                    go.Scatter(name='Avg Quality', x=categories, y=avg_qualities, 
                              yaxis='y2', mode='lines+markers', line=dict(color='red'))
                ])
                
                fig.update_layout(
                    title='Article Distribution by Category',
                    xaxis_title='Category',
                    yaxis_title='Article Count',
                    yaxis2=dict(title='Average Quality Score', overlaying='y', side='right'),
                    hovermode='x unified'
                )
            
            return {
                'chart': json.loads(fig.to_json()),
                'data': {
                    'categories': categories,
                    'counts': counts,
                    'avg_qualities': avg_qualities
                }
            }
            
        except Exception as e:
            logger.error(f"Category analytics error: {e}")
            fig = go.Figure()
            fig.update_layout(
                title='Error Loading Category Data',
                annotations=[{
                    'text': f'Error: {str(e)}',
                    'xref': 'paper', 'yref': 'paper',
                    'showarrow': False, 'font': {'size': 14}
                }]
            )
            return {
                'chart': json.loads(fig.to_json()),
                'error': str(e)
            }

    def _get_sentiment_analytics(self):
        """Get sentiment analysis over time"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT 
                    DATE(processing_timestamp) as date,
                    sentiment_label,
                    COUNT(*) as count
                FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY DATE(processing_timestamp), sentiment_label
                ORDER BY date, sentiment_label
            """)
            
            data = {}
            for row in cur.fetchall():
                date = row[0].isoformat()
                sentiment = row[1] or 'neutral'
                count = row[2]
                
                if date not in data:
                    data[date] = {'positive': 0, 'negative': 0, 'neutral': 0}
                
                data[date][sentiment] = count
            
            dates = sorted(data.keys())
            if not dates:
                return self._get_default_sentiment_analytics()
                
            all_sentiments = ['positive', 'negative', 'neutral']
            for date in dates:
                for sentiment in all_sentiments:
                    if sentiment not in data[date]:
                        data[date][sentiment] = 0
            
            positive = [data[date]['positive'] for date in dates]
            negative = [data[date]['negative'] for date in dates]
            neutral = [data[date]['neutral'] for date in dates]
            
            cur.close()
            conn.close()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(name='Positive', x=dates, y=positive, stackgroup='one', 
                                    line=dict(color='green'), fillcolor='rgba(0,255,0,0.3)'))
            fig.add_trace(go.Scatter(name='Neutral', x=dates, y=neutral, stackgroup='one',
                                    line=dict(color='blue'), fillcolor='rgba(0,0,255,0.3)'))
            fig.add_trace(go.Scatter(name='Negative', x=dates, y=negative, stackgroup='one',
                                    line=dict(color='red'), fillcolor='rgba(255,0,0,0.3)'))
            
            fig.update_layout(
                title='Sentiment Analysis Over Time (Last 7 Days)',
                xaxis_title='Date',
                yaxis_title='Number of Articles',
                hovermode='x unified'
            )
            
            return {
                'chart': json.loads(fig.to_json()),
                'data': {
                    'dates': dates,
                    'positive': positive,
                    'neutral': neutral,
                    'negative': negative
                }
            }
            
        except Exception as e:
            logger.error(f"Sentiment analytics error: {e}")
            return self._get_default_sentiment_analytics(str(e))

    def _get_default_sentiment_analytics(self, error_msg=None):
        """Return default sentiment analytics when data is unavailable"""
        fig = go.Figure()
        title = f'Error: {error_msg}' if error_msg else 'No Sentiment Data Available'
            
        fig.update_layout(
            title=title,
            annotations=[{
                'text': 'Sentiment analysis data not available',
                'xref': 'paper', 'yref': 'paper',
                'showarrow': False, 'font': {'size': 16}
            }]
        )
        return {
            'chart': json.loads(fig.to_json()),
            'error': error_msg or 'No data available'
        }

    def _get_domain_analytics(self):
        """Get domain performance analytics"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT 
                    domain,
                    COUNT(*) as article_count,
                    AVG(quality_score) as avg_quality,
                    AVG(content_length) as avg_length,
                    COUNT(CASE WHEN sentiment_label = 'positive' THEN 1 END) as positive_count
                FROM articles 
                GROUP BY domain
                HAVING COUNT(*) >= 1
                ORDER BY article_count DESC
                LIMIT 15
            """)
            
            domains = []
            article_counts = []
            avg_qualities = []
            avg_lengths = []
            positivity_rates = []
            
            for row in cur.fetchall():
                domains.append(row[0])
                article_counts.append(row[1])
                avg_qualities.append(float(round(row[2] or 0, 1)))
                avg_lengths.append(int(row[3] or 0))
                positivity_rate = (row[4] / row[1] * 100) if row[1] > 0 else 0
                positivity_rates.append(float(round(positivity_rate, 1)))
            
            cur.close()
            conn.close()
            
            if not domains:
                return self._get_default_domain_analytics("No domain data available")
            
            fig = go.Figure(data=[go.Scatter(
                x=domains,
                y=avg_qualities,
                mode='markers',
                marker=dict(
                    size=article_counts,
                    sizemode='area',
                    sizeref=2.*max(article_counts)/(40.**2) if article_counts else 1,
                    sizemin=4,
                    color=positivity_rates,
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Positivity %")
                ),
                text=[f"Articles: {count}<br>Avg Quality: {quality}<br>Positivity: {positivity}%"
                      for count, quality, positivity in zip(article_counts, avg_qualities, positivity_rates)],
                hovertemplate='<b>%{x}</b><br>%{text}<extra></extra>'
            )])
            
            fig.update_layout(
                title='Domain Performance Analysis',
                xaxis_title='Domain',
                yaxis_title='Average Quality Score',
                hovermode='closest'
            )
            
            return {
                'chart': json.loads(fig.to_json()),
                'data': {
                    'domains': domains,
                    'article_counts': article_counts,
                    'avg_qualities': avg_qualities,
                    'avg_lengths': avg_lengths,
                    'positivity_rates': positivity_rates
                }
            }
            
        except Exception as e:
            logger.error(f"Domain analytics error: {e}")
            return self._get_default_domain_analytics(str(e))

    def _get_default_domain_analytics(self, error_msg=None):
        """Return default domain analytics when data is unavailable"""
        fig = go.Figure()
        title = f'Error: {error_msg}' if error_msg else 'No Domain Data Available'
            
        fig.update_layout(
            title=title,
            annotations=[{
                'text': 'Domain analytics data not available',
                'xref': 'paper', 'yref': 'paper',
                'showarrow': False, 'font': {'size': 16}
            }]
        )
        return {
            'chart': json.loads(fig.to_json()),
            'error': error_msg or 'No data available'
        }
    
    def _check_database_schema(self):
        """Check if database has required schema"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name='articles'
            """)
            
            columns = {row[0]: row[1] for row in cur.fetchall()}
            logger.info(f"Database columns: {list(columns.keys())}")
            
            required_columns = ['article_id', 'title', 'domain', 'quality_score']
            missing_columns = [col for col in required_columns if col not in columns]
            
            if missing_columns:
                logger.warning(f"Missing columns in articles table: {missing_columns}")
            
            cur.close()
            conn.close()
            return columns
            
        except Exception as e:
            logger.error(f"Database schema check failed: {e}")
            return {}
    
    def run(self):
        """Run the web dashboard"""
        host = self.config['web']['host']
        port = self.config['web']['port']
        debug = self.config['web'].get('debug', False)
        
        logger.info(f"🚀 Starting OmniCrawler Web Dashboard on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)



def create_app():
    """Factory function for application creation"""
    return WebDashboard().app

if __name__ == '__main__':
    dashboard = WebDashboard()
    dashboard.run()

    