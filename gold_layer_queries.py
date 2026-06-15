"""
Gold Layer Query Templates
Storage & Lakehouse Architecture (Anggota 4)

SQL templates yang siap digunakan untuk:
1. Apache Superset (OLAP cube queries)
2. Grafana (time-series queries)
3. Custom dashboards
4. Ad-hoc analysis

Semua query read-only dan optimized untuk dashboard visualization.

Usage:
    from gold_layer_queries import QUERIES
    
    # Execute query dengan Spark SQL atau direct JDBC connection
    spark.sql(QUERIES['top_violations_hourly'])
    
    # Atau pakai template dengan parameter
    query = QUERIES['hourly_by_camera'].format(camera_id='CCTV_BASUKI_RAHMAT')
"""

# ============================================================================
# SQL QUERY TEMPLATES - READY FOR DASHBOARD
# ============================================================================

QUERIES = {
    # ========================================================================
    # 1. OVERVIEW & KEY METRICS
    # ========================================================================
    
    "total_violations_today": """
    SELECT 
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as affected_cameras,
        ROUND(AVG(avg_confidence), 4) as avg_confidence_score,
        MAX(window_end) as last_update_time
    FROM gold.daily
    WHERE event_date = CURRENT_DATE()
    """,
    
    "total_violations_last_24h": """
    SELECT 
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as affected_cameras,
        ROUND(AVG(avg_confidence), 4) as avg_confidence_score
    FROM gold.daily
    WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAY
    """,
    
    # ========================================================================
    # 2. HOURLY AGGREGATION (Real-time dashboard)
    # ========================================================================
    
    "hourly_summary": """
    SELECT 
        window_start,
        window_end,
        camera_id,
        location,
        vehicle_type,
        total_violations,
        ROUND(avg_confidence, 4) as avg_confidence,
        ROUND(max_confidence, 4) as max_confidence,
        ROUND(stddev_confidence, 4) as stddev_confidence
    FROM gold.hourly
    WHERE event_date = CURRENT_DATE()
    ORDER BY window_start DESC, total_violations DESC
    """,
    
    "hourly_by_camera": """
    SELECT 
        window_start,
        window_end,
        location,
        SUM(total_violations) as total_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence,
        COUNT(DISTINCT vehicle_type) as vehicle_types_detected
    FROM gold.hourly
    WHERE camera_id = '{camera_id}'
        AND event_date = CURRENT_DATE()
    GROUP BY window_start, window_end, location
    ORDER BY window_start DESC
    """,
    
    "hourly_by_vehicle": """
    SELECT 
        window_start,
        window_end,
        vehicle_type,
        total_violations,
        affected_cameras,
        ROUND(avg_confidence, 4) as avg_confidence
    FROM gold.vehicle
    WHERE event_date = CURRENT_DATE()
    ORDER BY window_start DESC, total_violations DESC
    """,
    
    # ========================================================================
    # 3. HOTSPOT ANALYSIS (Kamera dengan violation tertinggi)
    # ========================================================================
    
    "top_cameras_today": """
    SELECT 
        camera_id,
        location,
        SUM(total_violations) as total_violations,
        COUNT(*) as windows_with_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence,
        MAX(distinct_vehicle_types) as max_vehicle_types
    FROM gold.camera
    WHERE event_date = CURRENT_DATE()
    GROUP BY camera_id, location
    ORDER BY total_violations DESC
    LIMIT 10
    """,
    
    "top_cameras_last_7days": """
    SELECT 
        camera_id,
        location,
        SUM(total_violations) as total_violations,
        COUNT(*) as windows_with_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.camera
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY camera_id, location
    ORDER BY total_violations DESC
    LIMIT 10
    """,
    
    "camera_hourly_pattern": """
    SELECT 
        HOUR(window_start) as hour_of_day,
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as cameras_affected,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.hourly
    WHERE camera_id = '{camera_id}'
        AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY HOUR(window_start)
    ORDER BY hour_of_day ASC
    """,
    
    # ========================================================================
    # 4. VEHICLE TYPE ANALYSIS
    # ========================================================================
    
    "violations_by_vehicle_type": """
    SELECT 
        vehicle_type,
        SUM(total_violations) as total_violations,
        affected_cameras,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.vehicle
    WHERE event_date = CURRENT_DATE()
    GROUP BY vehicle_type, affected_cameras
    ORDER BY total_violations DESC
    """,
    
    "vehicle_type_daily_trend": """
    SELECT 
        event_date,
        vehicle_type,
        SUM(total_violations) as total_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.vehicle
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY event_date, vehicle_type
    ORDER BY event_date DESC, total_violations DESC
    """,
    
    "vehicle_by_camera": """
    SELECT 
        camera_id,
        location,
        vehicle_type,
        SUM(total_violations) as total_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.hourly
    WHERE event_date = CURRENT_DATE()
    GROUP BY camera_id, location, vehicle_type
    ORDER BY total_violations DESC
    """,
    
    # ========================================================================
    # 5. CONFIDENCE SCORE ANALYSIS (Quality metrics)
    # ========================================================================
    
    "confidence_statistics_hourly": """
    SELECT 
        window_start,
        window_end,
        vehicle_type,
        ROUND(avg_confidence, 4) as avg_confidence,
        ROUND(median_confidence, 4) as median_confidence,
        ROUND(p95_confidence, 4) as p95_confidence,
        ROUND(min_confidence, 4) as min_confidence,
        ROUND(max_confidence, 4) as max_confidence,
        sample_size as total_detections
    FROM gold.confidence
    WHERE event_date = CURRENT_DATE()
    ORDER BY window_start DESC
    """,
    
    "confidence_by_vehicle_type": """
    SELECT 
        vehicle_type,
        ROUND(AVG(avg_confidence), 4) as avg_confidence,
        ROUND(AVG(median_confidence), 4) as median_confidence,
        ROUND(AVG(p95_confidence), 4) as p95_confidence,
        SUM(sample_size) as total_samples
    FROM gold.confidence
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY vehicle_type
    ORDER BY avg_confidence DESC
    """,
    
    "low_confidence_detections": """
    SELECT 
        window_start,
        vehicle_type,
        ROUND(avg_confidence, 4) as avg_confidence,
        sample_size
    FROM gold.confidence
    WHERE event_date = CURRENT_DATE()
        AND avg_confidence < 0.30
    ORDER BY avg_confidence ASC
    """,
    
    # ========================================================================
    # 6. TIME SERIES & TRENDS
    # ========================================================================
    
    "daily_trend": """
    SELECT 
        event_date,
        total_violations,
        affected_cameras,
        ROUND(avg_confidence, 4) as avg_confidence,
        distinct_vehicle_types
    FROM gold.daily
    WHERE event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    ORDER BY event_date DESC
    """,
    
    "peak_hours": """
    SELECT 
        HOUR(window_start) as hour_of_day,
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as cameras_affected,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.hourly
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY HOUR(window_start)
    ORDER BY total_violations DESC
    """,
    
    "hourly_trend_current_day": """
    SELECT 
        window_start,
        window_end,
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as cameras_affected,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.hourly
    WHERE event_date = CURRENT_DATE()
    GROUP BY window_start, window_end
    ORDER BY window_start ASC
    """,
    
    # ========================================================================
    # 7. COMPARATIVE ANALYSIS
    # ========================================================================
    
    "camera_comparison": """
    SELECT 
        camera_id,
        location,
        SUM(total_violations) as total_violations,
        ROUND(AVG(avg_confidence), 4) as avg_confidence,
        MAX(distinct_vehicle_types) as vehicle_types
    FROM gold.camera
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY camera_id, location
    ORDER BY total_violations DESC
    """,
    
    "vehicle_comparison": """
    SELECT 
        vehicle_type,
        SUM(total_violations) as total_violations,
        COUNT(DISTINCT camera_id) as cameras_affected,
        ROUND(AVG(avg_confidence), 4) as avg_confidence
    FROM gold.vehicle
    WHERE event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY vehicle_type
    ORDER BY total_violations DESC
    """,
    
    # ========================================================================
    # 8. ANOMALY & OUTLIER DETECTION
    # ========================================================================
    
    "unusual_spike_detection": """
    WITH hourly_stats AS (
        SELECT 
            window_start,
            SUM(total_violations) as hourly_violations,
            AVG(SUM(total_violations)) OVER (ORDER BY window_start ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING) as avg_violations_24h
        FROM gold.hourly
        WHERE event_date >= CURRENT_DATE() - INTERVAL 1 DAY
        GROUP BY window_start
    )
    SELECT 
        window_start,
        hourly_violations,
        ROUND(avg_violations_24h, 0) as avg_violations_24h,
        ROUND((hourly_violations - avg_violations_24h) / NULLIF(avg_violations_24h, 0) * 100, 2) as pct_deviation
    FROM hourly_stats
    WHERE hourly_violations > avg_violations_24h * 1.5
    ORDER BY window_start DESC
    """,
    
    # ========================================================================
    # 9. SUMMARY STATISTICS
    # ========================================================================
    
    "summary_statistics": """
    SELECT 
        'Total Violations (Today)' as metric,
        CAST(SUM(total_violations) as STRING) as value
    FROM gold.daily
    WHERE event_date = CURRENT_DATE()
    UNION ALL
    SELECT 
        'Affected Cameras (Today)',
        CAST(COUNT(DISTINCT camera_id) as STRING)
    FROM gold.camera
    WHERE event_date = CURRENT_DATE()
    UNION ALL
    SELECT 
        'Avg Confidence Score',
        CAST(ROUND(AVG(avg_confidence), 4) as STRING)
    FROM gold.camera
    WHERE event_date = CURRENT_DATE()
    UNION ALL
    SELECT 
        'Most Common Vehicle Type',
        vehicle_type
    FROM (
        SELECT 
            vehicle_type,
            SUM(total_violations) as total,
            ROW_NUMBER() OVER (ORDER BY SUM(total_violations) DESC) as rn
        FROM gold.vehicle
        WHERE event_date = CURRENT_DATE()
        GROUP BY vehicle_type
    )
    WHERE rn = 1
    """,
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_query(query_name: str, **kwargs) -> str:
    """
    Get SQL query dengan parameter substitution
    
    Args:
        query_name: Key dari QUERIES dict
        **kwargs: Parameter untuk string formatting
        
    Returns:
        Formatted SQL query string
        
    Example:
        query = get_query('hourly_by_camera', camera_id='CCTV_BASUKI_RAHMAT')
    """
    if query_name not in QUERIES:
        raise ValueError(f"Query '{query_name}' not found. Available queries: {list(QUERIES.keys())}")
    
    query = QUERIES[query_name]
    
    # Only format if query has placeholders
    if kwargs and '{' in query:
        query = query.format(**kwargs)
    
    return query


def list_available_queries() -> list:
    """List semua available query names"""
    return sorted(QUERIES.keys())


def get_query_info(query_name: str) -> dict:
    """
    Get metadata tentang query
    
    Returns:
        Dictionary dengan info query
    """
    category_map = {
        'total_violations': 'Overview',
        'hourly': 'Hourly Aggregation',
        'top_cameras': 'Hotspot Analysis',
        'camera': 'Hotspot Analysis',
        'vehicle': 'Vehicle Analysis',
        'confidence': 'Quality Metrics',
        'trend': 'Trends',
        'peak': 'Trends',
        'comparison': 'Comparative Analysis',
        'spike': 'Anomaly Detection',
        'summary': 'Summary Statistics',
    }
    
    category = "Other"
    for key, val in category_map.items():
        if key in query_name:
            category = val
            break
    
    return {
        "query_name": query_name,
        "category": category,
        "has_parameters": '{' in QUERIES[query_name],
    }


# ============================================================================
# QUICK ACCESS DICTIONARIES
# ============================================================================

# Group queries by category
QUERIES_BY_CATEGORY = {
    "Overview": ["total_violations_today", "total_violations_last_24h"],
    "Hourly Aggregation": [k for k in QUERIES if 'hourly' in k],
    "Hotspot Analysis": [k for k in QUERIES if 'camera' in k or 'hotspot' in k],
    "Vehicle Analysis": [k for k in QUERIES if 'vehicle' in k],
    "Quality Metrics": [k for k in QUERIES if 'confidence' in k],
    "Trends": [k for k in QUERIES if 'trend' in k or 'peak' in k],
    "Anomaly Detection": [k for k in QUERIES if 'spike' in k or 'unusual' in k],
    "Summary": [k for k in QUERIES if 'summary' in k or 'statistics' in k],
}

if __name__ == "__main__":
    # Test: Print semua available queries
    print("Available Gold Layer Queries:")
    print("=" * 80)
    for category, queries in QUERIES_BY_CATEGORY.items():
        print(f"\n{category}:")
        for query in queries:
            print(f"  - {query}")
    
    print("\n" + "=" * 80)
    print(f"Total queries: {len(QUERIES)}")
