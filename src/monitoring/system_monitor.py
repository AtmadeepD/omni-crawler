import logging
import time
import psutil
from datetime import datetime, timedelta
from threading import Thread, Event
import json
import redis
from elasticsearch import Elasticsearch
import psycopg2
from prometheus_client import Gauge, Counter, Histogram
from typing import Dict
from src.alerts.alert_engine import AlertEngine

logger = logging.getLogger(__name__)

class SystemMonitor:
    def __init__(self, redis_host='localhost', es_host='localhost:9200', 
                 pg_host='localhost', pg_db='news_omnidb'):
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.es = Elasticsearch([f'http://{es_host}'])
        self.pg_config = {
            'host': pg_host,
            'database': pg_db,
            'user': 'crawler',
            'password': 'crawler123',
            'port': 5432
        }
        
        # Metrics
        self.cpu_usage = Gauge('system_cpu_percent', 'CPU usage percentage')
        self.memory_usage = Gauge('system_memory_percent', 'Memory usage percentage')
        self.disk_usage = Gauge('system_disk_percent', 'Disk usage percentage')
        self.article_count = Gauge('system_article_count', 'Total articles in database')
        self.crawl_success_rate = Gauge('crawl_success_rate', 'Crawl success rate')
        self.processing_latency = Histogram('processing_latency_seconds', 'Article processing latency')
        self.api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'status'])
        
        self.monitoring_data = {}
        self.is_running = False
        self.monitor_thread = None
        
    def start_monitoring(self, interval: int = 60):
        """Start background monitoring"""
        self.is_running = True
        self.monitor_thread = Thread(target=self._monitoring_loop, args=(interval,))
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        logger.info(f"🚀 System monitoring started with {interval}s interval")
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.is_running = False
        if self.monitor_thread:
            self.monitor_thread.join()
        logger.info("🛑 System monitoring stopped")
    
    def _monitoring_loop(self, interval: int):
        """Main monitoring loop"""
        while self.is_running:
            try:
                self._collect_system_metrics()
                self._collect_application_metrics()
                self._collect_database_metrics()
                self._collect_storage_metrics()
                self._update_prometheus_metrics()
                self._store_monitoring_data()
                
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                time.sleep(interval)
    
    def _collect_system_metrics(self):
        """Collect system-level metrics"""
        try:
            # CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # Network
            net_io = psutil.net_io_counters()
            
            # Store metrics for alert evaluation
            self.monitoring_data['system'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_used_gb': round(memory.used / (1024**3), 2),
                'memory_total_gb': round(memory.total / (1024**3), 2),
                'disk_percent': disk_percent,
                'disk_used_gb': round(disk.used / (1024**3), 2),
                'disk_total_gb': round(disk.total / (1024**3), 2),
                'network_bytes_sent': net_io.bytes_sent,
                'network_bytes_recv': net_io.bytes_recv
            }

            # Trigger alert evaluation
            self._evaluate_alerts()
            
        except Exception as e:
            logger.error(f"System metrics collection error: {e}")

    def _evaluate_alerts(self):
        """Evaluate metrics against alert rules"""
        try:
            from src.alerts.alert_engine import AlertEngine
            alert_engine = AlertEngine()
            alerts = alert_engine.evaluate_alerts(self.monitoring_data)
            
            for alert in alerts:
                logger.warning(f"🚨 ALERT: {alert['message']}")
                
        except Exception as e:
            logger.error(f"Alert evaluation failed: {e}")
    
    def _collect_application_metrics(self):
        """Collect application-level metrics"""
        try:
            # Get recent crawl stats from Redis
            recent_crawls = self.redis.lrange('recent_crawls', 0, 9)
            success_count = 0
            total_count = len(recent_crawls)
            
            for crawl_data in recent_crawls:
                try:
                    data = json.loads(crawl_data)
                    if data.get('success'):
                        success_count += 1
                except:
                    pass
            
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            # Get API request stats
            api_stats = {}
            for key in self.redis.scan_iter("api_requests:*"):
                endpoint = key.split(':')[1]
                count = self.redis.get(key)
                api_stats[endpoint] = int(count) if count else 0
            
            self.monitoring_data['application'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'crawl_success_rate': round(success_rate, 2),
                'recent_crawls_count': total_count,
                'api_requests': api_stats,
                'redis_connected': self.redis.ping(),
                'elasticsearch_connected': self.es.ping()
            }
            
        except Exception as e:
            logger.error(f"Application metrics collection error: {e}")
    
    def _collect_database_metrics(self):
        """Collect database metrics"""
        try:
            conn = psycopg2.connect(**self.pg_config)
            cur = conn.cursor()
            
            # Article counts
            cur.execute("SELECT COUNT(*) FROM articles")
            total_articles = cur.fetchone()[0]
            
            # Recent articles (last hour)
            cur.execute("""
                SELECT COUNT(*) FROM articles 
                WHERE processing_timestamp >= NOW() - INTERVAL '1 hour'
            """)
            recent_articles = cur.fetchone()[0]
            
            # Quality distribution
            cur.execute("""
                SELECT 
                    AVG(quality_score),
                    COUNT(CASE WHEN quality_score >= 80 THEN 1 END),
                    COUNT(CASE WHEN quality_score < 50 THEN 1 END)
                FROM articles
            """)
            quality_stats = cur.fetchone()
            
            # Domain diversity
            cur.execute("SELECT COUNT(DISTINCT domain) FROM articles")
            unique_domains = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            self.monitoring_data['database'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'total_articles': total_articles,
                'recent_articles_1h': recent_articles,
                'avg_quality': round(quality_stats[0] or 0, 2),
                'high_quality_count': quality_stats[1] or 0,
                'low_quality_count': quality_stats[2] or 0,
                'unique_domains': unique_domains,
                'postgres_connected': True
            }
            
        except Exception as e:
            logger.error(f"Database metrics collection error: {e}")
            self.monitoring_data['database'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'postgres_connected': False,
                'error': str(e)
            }
    
    def _collect_storage_metrics(self):
        """Collect storage system metrics"""
        try:
            # Elasticsearch metrics
            es_info = self.es.info()
            es_health = self.es.cluster.health()
            es_stats = self.es.indices.stats(index='news-articles')
            
            # Redis metrics
            redis_info = self.redis.info()
            
            self.monitoring_data['storage'] = {
                'timestamp': datetime.utcnow().isoformat(),
                'elasticsearch': {
                    'cluster_name': es_info['cluster_name'],
                    'status': es_health['status'],
                    'number_of_nodes': es_health['number_of_nodes'],
                    'document_count': es_stats['indices']['news-articles']['total']['docs']['count'],
                    'index_size_bytes': es_stats['indices']['news-articles']['total']['store']['size_in_bytes']
                },
                'redis': {
                    'connected_clients': redis_info['connected_clients'],
                    'used_memory_human': redis_info['used_memory_human'],
                    'keyspace_hits': redis_info['keyspace_hits'],
                    'keyspace_misses': redis_info['keyspace_misses']
                }
            }
            
        except Exception as e:
            logger.error(f"Storage metrics collection error: {e}")
    
    def _update_prometheus_metrics(self):
        """Update Prometheus metrics with current data"""
        try:
            # System metrics
            if 'system' in self.monitoring_data:
                sys_data = self.monitoring_data['system']
                self.cpu_usage.set(sys_data['cpu_percent'])
                self.memory_usage.set(sys_data['memory_percent'])
                self.disk_usage.set(sys_data['disk_percent'])
            
            # Application metrics
            if 'database' in self.monitoring_data:
                db_data = self.monitoring_data['database']
                self.article_count.set(db_data['total_articles'])
            
            if 'application' in self.monitoring_data:
                app_data = self.monitoring_data['application']
                self.crawl_success_rate.set(app_data['crawl_success_rate'])
                
        except Exception as e:
            logger.error(f"Prometheus metrics update error: {e}")
    
    def _store_monitoring_data(self):
        """Store monitoring data in Redis for dashboard"""
        try:
            # Store current snapshot
            self.redis.setex(
                'monitoring:current',
                300,  # 5 minutes TTL
                json.dumps(self.monitoring_data)
            )
            
            # Store historical data (keep last 24 hours)
            timestamp = datetime.utcnow().isoformat()
            historical_key = f"monitoring:history:{timestamp}"
            self.redis.setex(historical_key, 86400, json.dumps(self.monitoring_data))
            
            # Trim old historical data
            history_keys = self.redis.keys('monitoring:history:*')
            if len(history_keys) > 1440:  # Keep max 24 hours of data (1 per minute)
                history_keys.sort()
                keys_to_delete = history_keys[:-1440]
                if keys_to_delete:
                    self.redis.delete(*keys_to_delete)
                    
        except Exception as e:
            logger.error(f"Monitoring data storage error: {e}")
    
    def get_current_metrics(self) -> Dict:
        """Get current monitoring metrics"""
        try:
            current_data = self.redis.get('monitoring:current')
            if current_data:
                return json.loads(current_data)
            return self.monitoring_data
        except Exception as e:
            logger.error(f"Get current metrics error: {e}")
            return {}
    
    def get_health_status(self) -> Dict:
        """Get system health status"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'services': {}
        }
        
        try:
            # Check PostgreSQL
            conn = psycopg2.connect(**self.pg_config)
            conn.close()
            health['services']['postgresql'] = 'healthy'
        except Exception as e:
            health['services']['postgresql'] = f'unhealthy: {str(e)}'
            health['status'] = 'degraded'
        
        try:
            # Check Elasticsearch
            if self.es.ping():
                health['services']['elasticsearch'] = 'healthy'
            else:
                health['services']['elasticsearch'] = 'unhealthy'
                health['status'] = 'degraded'
        except Exception as e:
            health['services']['elasticsearch'] = f'unhealthy: {str(e)}'
            health['status'] = 'degraded'
        
        try:
            # Check Redis
            if self.redis.ping():
                health['services']['redis'] = 'healthy'
            else:
                health['services']['redis'] = 'unhealthy'
                health['status'] = 'degraded'
        except Exception as e:
            health['services']['redis'] = f'unhealthy: {str(e)}'
            health['status'] = 'degraded'
        
        return health
    
    def trigger_alert(self, alert_type: str, message: str, severity: str = 'warning'):
        """Trigger system alert"""
        alert = {
            'type': alert_type,
            'message': message,
            'severity': severity,
            'timestamp': datetime.utcnow().isoformat(),
            'acknowledged': False
        }
        
        try:
            # Store alert in Redis
            alert_key = f"alert:{datetime.utcnow().timestamp()}"
            self.redis.setex(alert_key, 86400, json.dumps(alert))  # 24 hours TTL
            
            # Add to alerts list
            self.redis.lpush('alerts:recent', json.dumps(alert))
            self.redis.ltrim('alerts:recent', 0, 99)  # Keep only 100 most recent
            
            logger.warning(f"🚨 ALERT [{severity.upper()}]: {alert_type} - {message}")
            
        except Exception as e:
            logger.error(f"Alert triggering error: {e}")
    
    def check_and_trigger_alerts(self, metrics: Dict):
        """Check metrics and trigger alerts if needed"""
        try:
            from src.alerts.alert_engine import AlertEngine
            alert_engine = AlertEngine()
            alerts = alert_engine.evaluate_alerts(metrics)
            
            for alert in alerts:
                logger.warning(f"Alert triggered: {alert['message']}")
                
        except Exception as e:
            logger.error(f"Alert checking failed: {e}")