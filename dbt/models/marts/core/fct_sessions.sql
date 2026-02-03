{{
  config(
    materialized='incremental',
    unique_key='session_id',
    partition_by={
      'field': 'session_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['user_id', 'session_date'],
    incremental_strategy='insert_overwrite',
    tags=['marts', 'core', 'facts']
  )
}}

/*
  Fact Table: User Sessions
  
  Purpose:
    Session-level fact table for user engagement analytics.
    One row per session with aggregated metrics.
  
  Grain:
    One row per session (session_id)
  
  Source:
    int_user_sessions (ephemeral intermediate model)
  
  Metrics:
    - Session duration
    - Events per session
    - Songs played
    - Pages visited
    - Engagement score
  
  Incremental Strategy:
    - Partition by session_date
    - Cluster by user_id, session_date
    - Reprocesses last 3 days on each run
*/

WITH sessions AS (
  SELECT
    -- Primary key
    session_id,
    
    -- Foreign key
    user_id,
    
    -- User attributes (denormalized)
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    
    -- Location dimensions
    city,
    state,
    
    -- Device dimension
    user_agent,
    device_type,
    
    -- Time dimensions
    session_start_timestamp,
    session_end_timestamp,
    session_date,
    session_start_hour,
    
    -- Session metrics
    session_duration_seconds,
    session_duration_minutes,
    total_events,
    songs_played,
    unique_pages_visited,
    
    -- Derived metrics
    engagement_score,
    session_length_category,
    
    -- Flags
    has_song_plays,
    is_long_session,
    
    -- Additional context
    pages_visited,
    
    -- Calculated fields
    CASE
      WHEN songs_played > 0 
      THEN ROUND(session_duration_seconds / songs_played, 2)
      ELSE NULL
    END AS avg_seconds_per_song,
    
    CASE
      WHEN total_events > 0 
      THEN ROUND(session_duration_seconds / total_events, 2)
      ELSE NULL
    END AS avg_seconds_per_event,
    
    -- Session classification
    CASE
      WHEN songs_played = 0 THEN 'browsing'
      WHEN songs_played BETWEEN 1 AND 5 THEN 'light_listening'
      WHEN songs_played BETWEEN 6 AND 20 THEN 'moderate_listening'
      ELSE 'heavy_listening'
    END AS listening_intensity,
    
    -- Engagement tier
    CASE
      WHEN engagement_score < 10 THEN 'low'
      WHEN engagement_score < 30 THEN 'medium'
      WHEN engagement_score < 60 THEN 'high'
      ELSE 'very_high'
    END AS engagement_tier,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM {{ ref('int_user_sessions') }}
  
  {% if is_incremental() %}
    -- Incremental: only process last N days (lookback window)
    WHERE session_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
)

SELECT * FROM sessions
