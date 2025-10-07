import logging
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import redis
import psycopg2
from jinja2 import Template

logger = logging.getLogger(__name__)

@dataclass
class AlertRule:
    id: str
    name: str
    condition: str
    threshold: float
    metric: str
    severity: str
    enabled: bool
    cooldown_minutes: int
    channels: List[str]
    last_triggered: Optional[datetime] = None

class AlertEngine:
    def __init__(self, redis_host='localhost', pg_host='localhost'):
        self.redis = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.pg_config = {
            'host': pg_host,
            'database': 'news_omnidb',
            'user': 'crawler',
            'password': 'crawler123',
            'port': 5432
        }
        
        # Alert rules storage
        self.rules: Dict[str, AlertRule] = {}
        self.load_default_rules()
        
        # Notification channels
        self.smtp_config = None
        self.webhook_url = None
        self.slack_webhook = None
        
    def load_default_rules(self):
        """Load default alert rules"""
        default_rules = [
            AlertRule(
                id="cpu_high",
                name="High CPU Usage",
                condition=">",
                threshold=85.0,
                metric="system.cpu_percent",
                severity="warning",
                enabled=True,
                cooldown_minutes=30,
                channels=["dashboard", "email"]
            ),
            AlertRule(
                id="memory_high",
                name="High Memory Usage", 
                condition=">",
                threshold=90.0,
                metric="system.memory_percent",
                severity="warning",
                enabled=True,
                cooldown_minutes=30,
                channels=["dashboard", "email"]
            ),
            AlertRule(
                id="crawl_failure",
                name="Crawl Failure Rate High",
                condition=">",
                threshold=20.0,
                metric="application.crawl_success_rate",
                severity="error", 
                enabled=True,
                cooldown_minutes=15,
                channels=["dashboard", "email", "slack"]
            ),
            AlertRule(
                id="low_articles",
                name="Low Article Volume",
                condition="<", 
                threshold=10.0,
                metric="database.recent_articles_1h",
                severity="warning",
                enabled=True,
                cooldown_minutes=60,
                channels=["dashboard"]
            ),
            AlertRule(
                id="service_down",
                name="Service Unavailable",
                condition="==",
                threshold=0.0,
                metric="service.available",
                severity="critical",
                enabled=True,
                cooldown_minutes=5,
                channels=["dashboard", "email", "slack", "webhook"]
            )
        ]
        
        for rule in default_rules:
            self.rules[rule.id] = rule
    
    def evaluate_alerts(self, metrics: Dict):
        """Evaluate all alert rules against current metrics"""
        triggered_alerts = []
        
        for rule_id, rule in self.rules.items():
            if not rule.enabled:
                continue
                
            # Check cooldown period
            if rule.last_triggered:
                cooldown_end = rule.last_triggered + timedelta(minutes=rule.cooldown_minutes)
                if datetime.utcnow() < cooldown_end:
                    continue
            
            # Get metric value
            metric_value = self._get_metric_value(metrics, rule.metric)
            if metric_value is None:
                continue
            
            # Evaluate condition
            if self._evaluate_condition(metric_value, rule.condition, rule.threshold):
                alert = self._create_alert(rule, metric_value, metrics)
                triggered_alerts.append(alert)
                rule.last_triggered = datetime.utcnow()
                
                # Send notifications
                self._send_notifications(alert)
        
        return triggered_alerts
    
    def _get_metric_value(self, metrics: Dict, metric_path: str):
        """Get metric value from nested dictionary using dot notation"""
        try:
            keys = metric_path.split('.')
            value = metrics
            for key in keys:
                value = value.get(key, {})
            return float(value) if value != {} else None
        except (KeyError, TypeError, ValueError):
            return None
    
    def _evaluate_condition(self, value: float, condition: str, threshold: float) -> bool:
        """Evaluate alert condition"""
        if condition == ">":
            return value > threshold
        elif condition == ">=":
            return value >= threshold
        elif condition == "<":
            return value < threshold
        elif condition == "<=":
            return value <= threshold
        elif condition == "==":
            return value == threshold
        elif condition == "!=":
            return value != threshold
        return False
    
    def _create_alert(self, rule: AlertRule, current_value: float, metrics: Dict) -> Dict:
        """Create alert object"""
        return {
            'id': f"alert_{datetime.utcnow().timestamp()}",
            'rule_id': rule.id,
            'rule_name': rule.name,
            'severity': rule.severity,
            'message': self._generate_alert_message(rule, current_value),
            'current_value': current_value,
            'threshold': rule.threshold,
            'condition': rule.condition,
            'timestamp': datetime.utcnow().isoformat(),
            'metrics_snapshot': {
                'system': metrics.get('system', {}),
                'application': metrics.get('application', {}),
                'database': metrics.get('database', {})
            },
            'channels': rule.channels,
            'acknowledged': False
        }
    
    def _generate_alert_message(self, rule: AlertRule, current_value: float) -> str:
        """Generate human-readable alert message"""
        messages = {
            'cpu_high': f"CPU usage is {current_value}% (threshold: {rule.threshold}%)",
            'memory_high': f"Memory usage is {current_value}% (threshold: {rule.threshold}%)",
            'crawl_failure': f"Crawl success rate is {current_value}% (threshold: {rule.threshold}%)",
            'low_articles': f"Only {current_value} articles processed in last hour (threshold: {rule.threshold})",
            'service_down': f"Service is unavailable (current status: {current_value})"
        }
        
        return messages.get(rule.id, f"Alert triggered: {current_value} {rule.condition} {rule.threshold}")
    
    def _send_notifications(self, alert: Dict):
        """Send notifications through configured channels"""
        for channel in alert['channels']:
            try:
                if channel == 'dashboard':
                    self._send_dashboard_alert(alert)
                elif channel == 'email' and self.smtp_config:
                    self._send_email_alert(alert)
                elif channel == 'slack' and self.slack_webhook:
                    self._send_slack_alert(alert)
                elif channel == 'webhook' and self.webhook_url:
                    self._send_webhook_alert(alert)
            except Exception as e:
                logger.error(f"Failed to send {channel} alert: {e}")
    
    def _send_dashboard_alert(self, alert: Dict):
        """Store alert in Redis for dashboard display"""
        try:
            # Store individual alert
            alert_key = f"alert:{alert['id']}"
            self.redis.setex(alert_key, 86400, json.dumps(alert))  # 24 hours
            
            # Add to recent alerts list
            self.redis.lpush('alerts:recent', json.dumps(alert))
            self.redis.ltrim('alerts:recent', 0, 99)  # Keep 100 most recent
            
            # Update alert counts
            severity_key = f"alerts:count:{alert['severity']}"
            self.redis.incr(severity_key)
            self.redis.expire(severity_key, 86400)  # 24 hours
            
        except Exception as e:
            logger.error(f"Dashboard alert storage failed: {e}")
    
    def _send_email_alert(self, alert: Dict):
        """Send email alert"""
        if not self.smtp_config:
            return
            
        try:
            message = MIMEMultipart()
            message['Subject'] = f"[{alert['severity'].upper()}] {alert['rule_name']}"
            message['From'] = self.smtp_config['from_email']
            message['To'] = self.smtp_config['to_email']
            
            # Create HTML email
            html_template = """
            <html>
            <body>
                <h2>🚨 OmniCrawler Alert</h2>
                <div style="border-left: 4px solid {{ color }}; padding-left: 15px;">
                    <h3>{{ rule_name }}</h3>
                    <p><strong>Severity:</strong> <span style="color: {{ color }}">{{ severity }}</span></p>
                    <p><strong>Message:</strong> {{ message }}</p>
                    <p><strong>Current Value:</strong> {{ current_value }}</p>
                    <p><strong>Threshold:</strong> {{ threshold }}</p>
                    <p><strong>Time:</strong> {{ timestamp }}</p>
                </div>
                <hr>
                <p><small>This alert was triggered by rule: {{ rule_id }}</small></p>
            </body>
            </html>
            """
            
            severity_colors = {
                'critical': '#dc3545',
                'error': '#dc3545', 
                'warning': '#ffc107',
                'info': '#17a2b8'
            }
            
            template = Template(html_template)
            html_content = template.render(
                color=severity_colors.get(alert['severity'], '#6c757d'),
                **alert
            )
            
            message.attach(MIMEText(html_content, 'html'))
            
            with smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config['smtp_port']) as server:
                server.starttls()
                server.login(self.smtp_config['username'], self.smtp_config['password'])
                server.send_message(message)
                
            logger.info(f"Email alert sent: {alert['rule_name']}")
            
        except Exception as e:
            logger.error(f"Email alert failed: {e}")
    
    def _send_slack_alert(self, alert: Dict):
        """Send Slack alert via webhook"""
        if not self.slack_webhook:
            return
            
        try:
            severity_colors = {
                'critical': '#dc3545',
                'error': '#dc3545',
                'warning': '#ffc107', 
                'info': '#17a2b8'
            }
            
            slack_message = {
                'attachments': [
                    {
                        'color': severity_colors.get(alert['severity'], '#6c757d'),
                        'title': f"🚨 {alert['rule_name']}",
                        'text': alert['message'],
                        'fields': [
                            {
                                'title': 'Current Value',
                                'value': str(alert['current_value']),
                                'short': True
                            },
                            {
                                'title': 'Threshold', 
                                'value': str(alert['threshold']),
                                'short': True
                            },
                            {
                                'title': 'Severity',
                                'value': alert['severity'].upper(),
                                'short': True
                            }
                        ],
                        'ts': datetime.utcnow().timestamp()
                    }
                ]
            }
            
            response = requests.post(
                self.slack_webhook,
                json=slack_message,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info(f"Slack alert sent: {alert['rule_name']}")
            else:
                logger.error(f"Slack alert failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
    
    def _send_webhook_alert(self, alert: Dict):
        """Send webhook alert"""
        if not self.webhook_url:
            return
            
        try:
            response = requests.post(
                self.webhook_url,
                json=alert,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code in [200, 201, 202]:
                logger.info(f"Webhook alert sent: {alert['rule_name']}")
            else:
                logger.error(f"Webhook alert failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")
    
    def configure_smtp(self, smtp_server: str, smtp_port: int, username: str, password: str, 
                      from_email: str, to_email: str):
        """Configure SMTP for email alerts"""
        self.smtp_config = {
            'smtp_server': smtp_server,
            'smtp_port': smtp_port,
            'username': username,
            'password': password,
            'from_email': from_email,
            'to_email': to_email
        }
    
    def configure_slack(self, webhook_url: str):
        """Configure Slack webhook"""
        self.slack_webhook = webhook_url
    
    def configure_webhook(self, webhook_url: str):
        """Configure general webhook"""
        self.webhook_url = webhook_url
    
    def get_recent_alerts(self, limit: int = 50) -> List[Dict]:
        """Get recent alerts from Redis"""
        try:
            alerts_data = self.redis.lrange('alerts:recent', 0, limit - 1)
            alerts = []
            
            for alert_json in alerts_data:
                try:
                    alerts.append(json.loads(alert_json))
                except json.JSONDecodeError:
                    continue
            
            return alerts
            
        except Exception as e:
            logger.error(f"Failed to get recent alerts: {e}")
            return []
    
    def acknowledge_alert(self, alert_id: str):
        """Mark alert as acknowledged"""
        try:
            alert_key = f"alert:{alert_id}"
            alert_data = self.redis.get(alert_key)
            
            if alert_data:
                alert = json.loads(alert_data)
                alert['acknowledged'] = True
                alert['acknowledged_at'] = datetime.utcnow().isoformat()
                
                self.redis.setex(alert_key, 86400, json.dumps(alert))
                
                # Also update in recent alerts list
                recent_alerts = self.redis.lrange('alerts:recent', 0, -1)
                for i, recent_alert_json in enumerate(recent_alerts):
                    recent_alert = json.loads(recent_alert_json)
                    if recent_alert.get('id') == alert_id:
                        recent_alert['acknowledged'] = True
                        recent_alert['acknowledged_at'] = datetime.utcnow().isoformat()
                        self.redis.lset('alerts:recent', i, json.dumps(recent_alert))
                        break
                        
        except Exception as e:
            logger.error(f"Failed to acknowledge alert {alert_id}: {e}")
    
    def get_alert_stats(self) -> Dict:
        """Get alert statistics"""
        try:
            stats = {}
            severities = ['critical', 'error', 'warning', 'info']
            
            for severity in severities:
                count = self.redis.get(f"alerts:count:{severity}")
                stats[severity] = int(count) if count else 0
            
            stats['total'] = sum(stats.values())
            stats['unacknowledged'] = len([
                alert for alert in self.get_recent_alerts(1000)
                if not alert.get('acknowledged', False)
            ])
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get alert stats: {e}")
            return {}