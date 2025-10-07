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
        logging.StreamHandler(sys.stdout)  # Use stdout for better encoding
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

        # Integrate advanced search and monitoring
        self.search_engine = AdvancedSearchEngine()
        self.system_monitor = SystemMonitor()
        self.system_monitor.start_monitoring(interval=60)  # Monitor every 60 seconds

        # Integrate alert engine and data exporter
        self.alert_engine = AlertEngine()
        self.data_exporter = DataExporter()

        # Configure basic email alerts (optional)
        # self.alert_engine.configure_smtp(
        #     smtp_server='smtp.gmail.com',
        #     smtp_port=587,
        #     username='your-email@gmail.com',
        #     password='your-app-password',
        #     from_email='alerts@omnicrawler.com',
        #     to_email='admin@yourcompany.com'
        # )

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
        """Setup all application routes"""
        
        @self.app.before_request
        def before_request():
            request.start_time = time.time()
        
        @self.app.after_request
        def after_request(response):
            # Metrics tracking
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
        
        # Main routes
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
        
        @self.app.route('/api/health')
        def health_check():
            """Health check endpoint"""
            health_status = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat(),
                'version': '1.0.0',
                'services': self._check_services()
            }
            return jsonify(health_status)
        
        @self.app.route('/api/stats/overview')
        def stats_overview():
            """Comprehensive system statistics"""
            try:
                stats = self._get_system_stats()
                return jsonify(stats)
            except Exception as e:
                logger.error(f"Stats overview error: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/articles')
        def get_articles():
            """Get paginated articles with filtering"""
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
            """Search articles with Elasticsearch"""
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
        
        @self.app.route('/api/analytics/categories')
        def analytics_categories():
            """Category distribution analytics"""
            try:
                data = self._get_category_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Category analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        @self.app.route('/api/analytics/sentiment')
        def analytics_sentiment():
            """Sentiment analysis over time"""
            try:
                data = self._get_sentiment_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Sentiment analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        @self.app.route('/api/analytics/domains')
        def analytics_domains():
            """Domain performance analytics"""
            try:
                data = self._get_domain_analytics()
                return jsonify(data)
            except Exception as e:
                logger.error(f"Domain analytics error: {e}")
                return jsonify({'error': 'Analytics service unavailable'}), 503
        
        @self.app.route('/api/search/advanced')
        def advanced_search():
            """Advanced search endpoint"""
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
            """System health status"""
            try:
                health = self.system_monitor.get_health_status()
                return jsonify(health)
            except Exception as e:
                logger.error(f"Health check error: {e}")
                return jsonify({'status': 'unknown', 'error': str(e)}), 500

        @self.app.route('/api/monitoring/metrics')
        def monitoring_metrics():
            """Current system metrics"""
            try:
                metrics = self.system_monitor.get_current_metrics()
                return jsonify(metrics)
            except Exception as e:
                logger.error(f"Metrics fetch error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/monitoring/alerts')
        def monitoring_alerts():
            """Get recent system alerts"""
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
            """Get recent alerts"""
            try:
                limit = request.args.get('limit', 50, type=int)
                alerts = self.alert_engine.get_recent_alerts(limit)
                return jsonify({'alerts': alerts})
            except Exception as e:
                logger.error(f"Get alerts error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/alerts/stats')
        def get_alert_stats():
            """Get alert statistics"""
            try:
                stats = self.alert_engine.get_alert_stats()
                return jsonify(stats)
            except Exception as e:
                logger.error(f"Get alert stats error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/alerts/<alert_id>/acknowledge', methods=['POST'])
        def acknowledge_alert(alert_id):
            """Acknowledge an alert"""
            try:
                self.alert_engine.acknowledge_alert(alert_id)
                return jsonify({'status': 'acknowledged'})
            except Exception as e:
                logger.error(f"Acknowledge alert error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/export/articles/csv')
        def export_articles_csv():
            """Export articles as CSV"""
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
            """Export articles as JSON"""
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

        @self.app.route('/api/export/analytics')
        def export_analytics():
            """Export analytics report"""
            try:
                days = request.args.get('days', 7, type=int)
                return self.data_exporter.export_analytics_report(days)
            except Exception as e:
                logger.error(f"Analytics export error: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/export/full-dump')
        def export_full_dump():
            """Export full database dump"""
            try:
                return self.data_exporter.export_full_database_dump()
            except Exception as e:
                logger.error(f"Full dump export error: {e}")
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
        
        # Check PostgreSQL
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            services['postgresql'] = 'healthy'
            conn.close()
        except Exception as e:
            services['postgresql'] = f'unhealthy: {str(e)}'
        
        # Check Elasticsearch
        try:
            es = self._get_es_connection()
            services['elasticsearch'] = 'healthy'
        except Exception as e:
            services['elasticsearch'] = f'unhealthy: {str(e)}'
        
        # Check Redis (simplified)
        services['redis'] = 'healthy'  # We'll implement proper check
        
        return services
    
    def _get_system_stats(self):
        """Get comprehensive system statistics"""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        stats = {}
        
        # Basic counts
        cur.execute("SELECT COUNT(*) FROM articles")
        stats['total_articles'] = cur.fetchone()[0]
        ARTICLE_COUNT.set(stats['total_articles'])
        
        cur.execute("SELECT COUNT(DISTINCT domain) FROM articles")
        stats['unique_domains'] = cur.fetchone()[0]
        
        # Quality distribution
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
        
        # Recent activity
        cur.execute("""
            SELECT COUNT(*) FROM articles 
            WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
        """)
        stats['recent_articles_1h'] = cur.fetchone()[0]
        
        # Category distribution
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
        """Get paginated articles with filtering"""
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        # Build WHERE clause
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
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM articles WHERE {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()[0]
        
        # Get paginated results
        offset = (page - 1) * per_page
        query = f"""
            SELECT 
                article_id, title, url, domain, authors, 
                quality_score, category, sentiment_label,
                processing_timestamp, content_length
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
                'content_length': row[9] or 0
            })
        
        cur.close()
        conn.close()
        
        return articles, total
    
    def _search_articles(self, query, size):
        """Search articles using Elasticsearch"""
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
                'score': hit['_score'],
                'highlight': highlight
            })
        
        return articles
    
    def _get_category_analytics(self):
        """Get category distribution analytics with robust error handling"""
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
            
            # Create Plotly chart with better error handling
            if not categories:
                # Return empty chart if no data
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
            # Return error chart
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
        """Get sentiment analysis over time with robust error handling"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Check if sentiment_label column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='articles' AND column_name='sentiment_label'
            """)
            
            if not cur.fetchone():
                logger.warning("sentiment_label column not found, using default data")
                return self._get_default_sentiment_analytics()
            
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
            
            # Process data for time series
            data = {}
            for row in cur.fetchall():
                date = row[0].isoformat()
                sentiment = row[1] or 'neutral'
                count = row[2]
                
                if date not in data:
                    data[date] = {'positive': 0, 'negative': 0, 'neutral': 0}
                
                data[date][sentiment] = count
            
            # Fill missing dates with zeros
            dates = sorted(data.keys())
            if not dates:
                return self._get_default_sentiment_analytics()
                
            # Ensure we have all sentiment types for each date
            all_sentiments = ['positive', 'negative', 'neutral']
            for date in dates:
                for sentiment in all_sentiments:
                    if sentiment not in data[date]:
                        data[date][sentiment] = 0
            
            # Convert to lists for charting
            positive = [data[date]['positive'] for date in dates]
            negative = [data[date]['negative'] for date in dates]
            neutral = [data[date]['neutral'] for date in dates]
            
            cur.close()
            conn.close()
            
            # Create stacked area chart
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
        if error_msg:
            title = f'Error: {error_msg}'
        else:
            title = 'No Sentiment Data Available'
            
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
        """Get domain performance analytics with robust error handling"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Check if required columns exist
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='articles' AND column_name IN ('sentiment_label', 'content_length')
            """)
            columns = [row[0] for row in cur.fetchall()]
            
            has_sentiment = 'sentiment_label' in columns
            has_content_length = 'content_length' in columns
            
            if has_sentiment and has_content_length:
                query = """
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
                """
            else:
                # Fallback query without sentiment and content_length
                query = """
                    SELECT 
                        domain,
                        COUNT(*) as article_count,
                        AVG(quality_score) as avg_quality
                    FROM articles 
                    GROUP BY domain
                    HAVING COUNT(*) >= 1
                    ORDER BY article_count DESC
                    LIMIT 15
                """
            
            cur.execute(query)
            
            domains = []
            article_counts = []
            avg_qualities = []
            avg_lengths = []
            positivity_rates = []
            
            for row in cur.fetchall():
                domains.append(row[0])
                article_counts.append(row[1])
                avg_qualities.append(float(round(row[2] or 0, 1)))
                
                if has_content_length:
                    avg_lengths.append(int(row[3] or 0))
                else:
                    avg_lengths.append(0)
                    
                if has_sentiment:
                    positivity_rate = (row[4] / row[1] * 100) if row[1] > 0 else 0
                    positivity_rates.append(float(round(positivity_rate, 1)))
                else:
                    positivity_rates.append(0)
            
            cur.close()
            conn.close()
            
            if not domains:
                return self._get_default_domain_analytics("No domain data available")
            
            # Create bubble chart
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
        if error_msg:
            title = f'Error: {error_msg}'
        else:
            title = 'No Domain Data Available'
            
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
            
            # Check for required columns
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