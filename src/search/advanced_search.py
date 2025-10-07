import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from elasticsearch import Elasticsearch
from flask import request, jsonify
import re

logger = logging.getLogger(__name__)

class AdvancedSearchEngine:
    def __init__(self, es_host='localhost:9200'):
        self.es = Elasticsearch([f'http://{es_host}'])
        self.search_analytics = {}
    
    def search_articles(self, search_params: Dict) -> Dict:
        """
        Advanced article search with multiple filters and analytics
        """
        try:
            # Build Elasticsearch query
            es_query = self._build_advanced_query(search_params)
            
            # Execute search
            response = self.es.search(
                index='news-articles',
                body=es_query
            )
            
            # Process results
            results = self._process_search_results(response, search_params)
            
            # Update search analytics
            self._update_search_analytics(search_params, len(results['articles']))
            
            return results
            
        except Exception as e:
            logger.error(f"Advanced search error: {e}")
            return {'articles': [], 'total': 0, 'error': str(e)}
    
    def _build_advanced_query(self, params: Dict) -> Dict:
        """Build complex Elasticsearch query"""
        must_conditions = []
        should_conditions = []
        filter_conditions = []
        
        # Text search
        if params.get('query'):
            text_query = {
                'multi_match': {
                    'query': params['query'],
                    'fields': [
                        'title^3', 
                        'content^2', 
                        'key_phrases^2',
                        'authors^1.5',
                        'domain^1.2'
                    ],
                    'fuzziness': 'AUTO',
                    'operator': 'and' if params.get('exact_match') else 'or'
                }
            }
            must_conditions.append(text_query)
        
        # Domain filter
        if params.get('domains'):
            filter_conditions.append({
                'terms': {'domain': params['domains']}
            })
        
        # Category filter
        if params.get('categories'):
            filter_conditions.append({
                'terms': {'category': params['categories']}
            })
        
        # Date range filter
        date_range = {}
        if params.get('date_from'):
            date_range['gte'] = params['date_from']
        if params.get('date_to'):
            date_range['lte'] = params['date_to']
        
        if date_range:
            filter_conditions.append({
                'range': {'processing_timestamp': date_range}
            })
        
        # Quality score filter
        if params.get('min_quality'):
            filter_conditions.append({
                'range': {'quality_score': {'gte': params['min_quality']}}
            })
        
        # Sentiment filter
        if params.get('sentiment'):
            filter_conditions.append({
                'term': {'sentiment.label': params['sentiment']}
            })
        
        # Content length filter
        if params.get('min_length'):
            filter_conditions.append({
                'range': {'content_length': {'gte': params['min_length']}}
            })
        
        # Build final query
        query = {
            'query': {
                'bool': {
                    'must': must_conditions,
                    'should': should_conditions,
                    'filter': filter_conditions,
                    'minimum_should_match': 0
                }
            },
            'size': params.get('size', 20),
            'from': params.get('from', 0),
            'sort': self._get_sort_config(params),
            'highlight': {
                'fields': {
                    'title': {'number_of_fragments': 0},
                    'content': {'fragment_size': 150, 'number_of_fragments': 3},
                    'key_phrases': {'number_of_fragments': 0}
                },
                'pre_tags': ['<mark>'],
                'post_tags': ['</mark>']
            },
            'aggs': self._get_aggregations()
        }
        
        return query
    
    def _get_sort_config(self, params: Dict) -> List[Dict]:
        """Get sorting configuration"""
        sort_field = params.get('sort_by', 'processing_timestamp')
        sort_order = 'desc' if params.get('sort_order', 'desc') == 'desc' else 'asc'
        
        # Special sorting cases
        if sort_field == 'relevance' and params.get('query'):
            return [{'_score': {'order': 'desc'}}]
        elif sort_field == 'quality':
            return [{'quality_score': {'order': sort_order}}]
        elif sort_field == 'date':
            return [{'processing_timestamp': {'order': sort_order}}]
        elif sort_field == 'length':
            return [{'content_length': {'order': sort_order}}]
        else:
            return [{sort_field: {'order': sort_order}}]
    
    def _get_aggregations(self) -> Dict:
        """Get search aggregations for faceted search"""
        return {
            'domains': {
                'terms': {'field': 'domain', 'size': 10}
            },
            'categories': {
                'terms': {'field': 'category', 'size': 10}
            },
            'sentiments': {
                'terms': {'field': 'sentiment.label', 'size': 5}
            },
            'quality_ranges': {
                'range': {
                    'field': 'quality_score',
                    'ranges': [
                        {'to': 50},
                        {'from': 50, 'to': 70},
                        {'from': 70, 'to': 85},
                        {'from': 85}
                    ]
                }
            },
            'date_histogram': {
                'date_histogram': {
                    'field': 'processing_timestamp',
                    'calendar_interval': 'day',
                    'format': 'yyyy-MM-dd'
                }
            }
        }
    
    def _process_search_results(self, response: Dict, params: Dict) -> Dict:
        """Process and enrich search results"""
        articles = []
        
        for hit in response['hits']['hits']:
            source = hit['_source']
            highlight = hit.get('highlight', {})
            
            article = {
                'id': hit['_id'],
                'title': source.get('title', ''),
                'url': source.get('url', ''),
                'domain': source.get('domain', ''),
                'authors': source.get('authors', []),
                'category': source.get('category', 'general'),
                'quality_score': source.get('quality_score', 0),
                'sentiment': source.get('sentiment', {}).get('label', 'neutral'),
                'content_length': source.get('content_length', 0),
                'processing_timestamp': source.get('processing_timestamp'),
                'score': hit['_score'],
                'highlight': highlight,
                'entities': source.get('entities', {}),
                'key_phrases': source.get('key_phrases', [])[:5]
            }
            
            # Generate content preview
            article['content_preview'] = self._generate_content_preview(source, highlight)
            articles.append(article)
        
        # Process aggregations for faceted search
        aggregations = self._process_aggregations(response.get('aggregations', {}))
        
        return {
            'articles': articles,
            'total': response['hits']['total']['value'],
            'aggregations': aggregations,
            'search_params': params
        }
    
    def _generate_content_preview(self, source: Dict, highlight: Dict) -> str:
        """Generate intelligent content preview"""
        # Use highlighted content if available
        if 'content' in highlight:
            return '...'.join(highlight['content'])[:300] + '...'
        
        # Fallback to first part of content
        content = source.get('content', '')
        if len(content) > 300:
            return content[:300] + '...'
        
        return content
    
    def _process_aggregations(self, aggs: Dict) -> Dict:
        """Process Elasticsearch aggregations for frontend"""
        return {
            'domains': [{'key': bucket['key'], 'count': bucket['doc_count']} 
                       for bucket in aggs.get('domains', {}).get('buckets', [])],
            'categories': [{'key': bucket['key'], 'count': bucket['doc_count']} 
                         for bucket in aggs.get('categories', {}).get('buckets', [])],
            'sentiments': [{'key': bucket['key'], 'count': bucket['doc_count']} 
                          for bucket in aggs.get('sentiments', {}).get('buckets', [])],
            'quality_ranges': [
                {'range': f"{bucket.get('from', 0)}-{bucket.get('to', 50)}", 'count': bucket['doc_count']}
                for bucket in aggs.get('quality_ranges', {}).get('buckets', [])
            ]
        }
    
    def _update_search_analytics(self, params: Dict, result_count: int):
        """Update search analytics for monitoring"""
        search_key = params.get('query', 'empty_query')
        
        if search_key not in self.search_analytics:
            self.search_analytics[search_key] = {
                'count': 0,
                'total_results': 0,
                'last_searched': None
            }
        
        self.search_analytics[search_key]['count'] += 1
        self.search_analytics[search_key]['total_results'] += result_count
        self.search_analytics[search_key]['last_searched'] = datetime.utcnow().isoformat()
    
    def get_search_analytics(self) -> Dict:
        """Get search analytics data"""
        return self.search_analytics
    
    def get_popular_searches(self, limit: int = 10) -> List[Dict]:
        """Get most popular searches"""
        popular = sorted(
            self.search_analytics.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )[:limit]
        
        return [{'query': k, **v} for k, v in popular]