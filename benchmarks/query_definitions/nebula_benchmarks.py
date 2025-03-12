#!/usr/bin/env python3
"""
Benchmark queries for Nebula database tables.
"""
from typing import List, Dict, Any

from .base import BenchmarkQueryCollection


class NebulaBenchmarks(BenchmarkQueryCollection):
    """Benchmark queries for Nebula database tables."""
    
    @property
    def name(self) -> str:
        return "nebula_benchmarks"
    
    @property
    def description(self) -> str:
        return "Benchmark queries for Nebula database tables (crawls, visits, neighbors, etc.)"
    
    def get_queries(self) -> List[Dict[str, Any]]:
        """Return predefined benchmark queries for Nebula tables."""
        return [
            # Crawls table benchmarks
            {
                "name": "crawls_table_scan_full",
                "description": "Full table scan of the crawls table",
                "query": "SELECT * FROM nebula.crawls"
            },
            {
                "name": "crawls_table_scan_last_day",
                "description": "Last day table scan of the crawls table",
                "query": "SELECT * FROM nebula.crawls WHERE created_at >= today() - INTERVAL 1 DAY"
            },
            {
                "name": "crawls_table_scan_last_3days",
                "description": "Last 3 days table scan of the crawls table",
                "query": "SELECT * FROM nebula.crawls WHERE created_at >= today() - INTERVAL 3 DAY"
            },
            {
                "name": "crawls_count",
                "description": "Count of rows in crawls table",
                "query": "SELECT COUNT(*) FROM nebula.crawls"
            },
            {
                "name": "crawls_filter_by_state",
                "description": "Filter crawls by state",
                "query": "SELECT * FROM crawls WHERE state = 'succeeded'"
            },
            {
                "name": "crawls_recent_stats",
                "description": "Statistics from recent successful crawls",
                "query": """
                SELECT 
                    formatDateTime(created_at, '%Y-%m-%d') as day,
                    COUNT(*) as total_crawls,
                    AVG(crawled_peers) as avg_crawled_peers,
                    AVG(dialable_peers) as avg_dialable_peers,
                    AVG(undialable_peers) as avg_undialable_peers
                FROM nebula.crawls
                WHERE 
                    state = 'succeeded' AND 
                    created_at >= NOW() - INTERVAL 30 DAY
                GROUP BY day
                ORDER BY day DESC
                """
            },
            
            # Visits table benchmarks
            {
                "name": "visits_table_scan_full",
                "description": "Full table scan of the visits table",
                "query": "SELECT * FROM nebula.visits"
            },
            {
                "name": "visits_table_scan_last_day_start",
                "description": "Last day table scan of the visits table, by visit_started_at",
                "query": """
                    SELECT * FROM nebula.visits 
                    WHERE visit_started_at >= today() - INTERVAL 1 DAY 
                    """
            },
            {
                "name": "visits_table_scan_last_3days_start",
                "description": "Last 3 days table scan of the visits table, by visit_started_at",
                "query": """
                    SELECT * FROM nebula.visits 
                    WHERE visit_started_at >= today() - INTERVAL 3 DAY 
                    """
            },
            {
                "name": "visits_table_scan_last_day_end",
                "description": "Last day table scan of the visits table, by visit_ended_at",
                "query": """
                    SELECT * FROM nebula.visits 
                    WHERE visit_ended_at >= today() - INTERVAL 1 DAY 
                    """
            },
            {
                "name": "visits_table_scan_last_3days_end",
                "description": "Last 3 days table scan of the visits table, by visit_ended_at",
                "query": """
                    SELECT * FROM nebula.visits 
                    WHERE visit_ended_at >= today() - INTERVAL 3 DAY 
                    """
            },
            {
                "name": "visits_count_full",
                "description": "Count of rows in visits table",
                "query": "SELECT COUNT(*) FROM nebula.visits"
            },

            {
                "name": "visits_count_last_day_start",
                "description": "Last day Count of rows visits table, by visit_started_at",
                "query": """
                    SELECT COUNT(*) FROM nebula.visits 
                    WHERE visit_started_at >= today() - INTERVAL 1 DAY 
                    """
            },
            {
                "name": "visits_count_last_3days_start",
                "description": "Last 3 days Count of rows visits table, by visit_started_at",
                "query": """
                    SELECT COUNT(*) FROM nebula.visits 
                    WHERE visit_started_at >= today() - INTERVAL 3 DAY 
                    """
            },
            {
                "name": "visits_count_last_day_end",
                "description": "Last day Count of rows visits table, by visit_ended_at",
                "query": """
                    SELECT COUNT(*) FROM nebula.visits 
                    WHERE visit_ended_at >= today() - INTERVAL 1 DAY 
                    """
            },
            {
                "name": "visits_count_last_3days_end",
                "description": "Last 3 days Count of rows visits table, by visit_ended_at",
                "query": """
                    SELECT COUNT(*) FROM nebula.visits 
                    WHERE visit_ended_at >= today() - INTERVAL 3 DAY 
                    """
            },
            

            {
                "name": "visits_filter_by_crawl_id",
                "description": "Filter visits by crawl_id",
                "query": """
                SELECT * 
                FROM nebula.visits 
                WHERE crawl_id = (SELECT id FROM crawls ORDER BY created_at DESC LIMIT 1)
                LIMIT 10000
                """
            },
            {
                "name": "visits_recent_with_filtering",
                "description": "Recent visits with filtering",
                "query": """
                SELECT 
                    visit_started_at,
                    peer_id,
                    agent_version,
                    connect_maddr,
                    dial_errors
                FROM nebula.visits
                WHERE 
                    visit_started_at >= NOW() - INTERVAL 1 DAY AND
                    length(dial_errors) = 0
                ORDER BY visit_started_at DESC
                LIMIT 10000
                """
            },
            {
                "name": "visits_complex_json_extraction",
                "description": "Complex JSON extraction and filtering",
                "query": """
                SELECT 
                    visit_started_at,
                    peer_id,
                    JSONExtractString(toString(peer_properties), 'ip') AS ip
                FROM nebula.visits
                WHERE 
                    visit_started_at >= NOW() - INTERVAL 7 DAY AND
                    toString(peer_properties) LIKE '%ip%'
                ORDER BY visit_started_at DESC
                LIMIT 10000
                """
            },
            
            # Neighbors table benchmarks
            {
                "name": "neighbors_full_table_scan",
                "description": "Full table scan of the neighbors table",
                "query": "SELECT * FROM nebula.neighbors LIMIT 10000"
            },
            {
                "name": "neighbors_count",
                "description": "Count of rows in neighbors table",
                "query": "SELECT COUNT(*) FROM nebula.neighbors"
            },
            {
                "name": "neighbors_with_join",
                "description": "Neighbors with join to discovery_id_prefixes_x_peer_ids",
                "query": """
                SELECT 
                    n.crawl_id,
                    dp.peer_id as peer,
                    dn.peer_id as neighbor
                FROM nebula.neighbors n
                JOIN nebula.discovery_id_prefixes_x_peer_ids dp ON n.peer_discovery_id_prefix = dp.discovery_id_prefix
                JOIN nebula.discovery_id_prefixes_x_peer_ids dn ON n.neighbor_discovery_id_prefix = dn.discovery_id_prefix
                LIMIT 10000
                """
            },
             
            # Cross-table queries
        #    {
        #        "name": "complex_multi_table_query",
        #        "description": "Complex query across multiple tables",
        #        "query": """
        #        SELECT 
        #            c.id as crawl_id,
        #            c.created_at as crawl_date,
        #            COUNT(DISTINCT v.peer_id) as unique_peers,
        #            COUNT(DISTINCT n.neighbor_discovery_id_prefix) as total_neighbors
        #        FROM nebula.crawls c
        #        LEFT JOIN nebula.visits v ON c.id = v.crawl_id
        #        LEFT JOIN nebula.neighbors n ON c.id = n.crawl_id
        #        WHERE 
        #            c.state = 'succeeded' AND
        #            c.created_at >= NOW() - INTERVAL 30 DAY
        #        GROUP BY c.id, c.created_at
        #        ORDER BY c.created_at DESC
        #        LIMIT 10000
        #        """
        #    },
        
            {
                "name": "peer_connectivity_analysis",
                "description": "Analyze peer connectivity",
                "query": """
                WITH 
                    recent_crawl AS (
                        SELECT id 
                        FROM nebula.crawls 
                        WHERE state = 'succeeded' 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    ),
                    connected_peers AS (
                        SELECT 
                            peer_id,
                            length(protocols) as protocol_count,
                            length(dial_maddrs) as dial_addr_count,
                            length(filtered_maddrs) as filtered_addr_count,
                            length(extra_maddrs) as extra_addr_count,
                            connect_maddr IS NOT NULL as is_connected
                        FROM nebula.visits
                        WHERE crawl_id = (SELECT id FROM recent_crawl)
                    )
                SELECT
                    is_connected,
                    COUNT(*) as peer_count,
                    AVG(protocol_count) as avg_protocols,
                    AVG(dial_addr_count) as avg_dial_addrs,
                    AVG(filtered_addr_count) as avg_filtered_addrs,
                    AVG(extra_addr_count) as avg_extra_addrs
                FROM connected_peers
                GROUP BY is_connected
                """
            }
        ]