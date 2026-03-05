{{
  config(
    materialized='incremental',
    unique_key='metric_date',
    partition_by={
      'field': 'metric_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    incremental_strategy='insert_overwrite',
    tags=['marts', 'analytics', 'aggregation']
  )
}}

/*
  Analytics Aggregation: Daily Metrics
  
  Purpose:
    Daily platform-wide metrics for executive dashboards and reporting.
    High-level KPIs to track platform health and growth.
  
  Grain:
    One row per date
  
  Sources:
    - fct_song_plays (for song metrics)
    - fct_sessions (for session metrics)
    - dim_users (for user metrics)
  
  Metrics:
    - Total song plays
    - Unique active users
    - Total sessions
    - Average session duration
    - Subscription metrics
*/

WITH daily_song_plays AS (
  SELECT
    event_date AS metric_date,
    COUNT(*) AS total_songs_played,
    COUNT(DISTINCT user_id) AS active_users_songs,
    COUNT(DISTINCT session_id) AS sessions_with_songs,
    COUNT(DISTINCT artist_name) AS unique_artists_played,
    COUNT(DISTINCT song_identifier) AS unique_songs_played,
    SUM(CASE WHEN likely_skip THEN 1 ELSE 0 END) AS total_skips,
    SUM(CASE WHEN likely_complete_listen THEN 1 ELSE 0 END) AS total_complete_listens,
    AVG(song_duration_seconds) AS avg_song_duration,
    
    -- By subscription level
    SUM(CASE WHEN subscription_level = 'free' THEN 1 ELSE 0 END) AS plays_free_users,
    SUM(CASE WHEN subscription_level = 'paid' THEN 1 ELSE 0 END) AS plays_paid_users,
    
    -- By device
    SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) AS plays_mobile,
    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) AS plays_desktop,
    SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS plays_tablet
    
  FROM {{ ref('fct_song_plays') }}
  
  {% if is_incremental() %}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
  
  GROUP BY event_date
),

daily_sessions AS (
  SELECT
    session_date AS metric_date,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT user_id) AS active_users_sessions,
    AVG(session_duration_minutes) AS avg_session_duration_minutes,
    AVG(songs_played) AS avg_songs_per_session,
    AVG(engagement_score) AS avg_engagement_score,
    SUM(total_events) AS total_events,
    
    -- Session types
    SUM(CASE WHEN has_song_plays THEN 1 ELSE 0 END) AS sessions_with_music,
    SUM(CASE WHEN is_long_session THEN 1 ELSE 0 END) AS long_sessions,
    
    -- By device
    SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) AS sessions_mobile,
    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) AS sessions_desktop,
    SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS sessions_tablet
    
  FROM {{ ref('fct_sessions') }}
  
  {% if is_incremental() %}
    WHERE session_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
  
  GROUP BY session_date
),

daily_users AS (
  -- Count distinct registered users active each day (not guests)
  SELECT
    event_date AS metric_date,
    COUNT(DISTINCT user_id) AS total_registered_users,
    COUNT(DISTINCT CASE WHEN subscription_level = 'free' THEN user_id END) AS free_users,
    COUNT(DISTINCT CASE WHEN subscription_level = 'paid' THEN user_id END) AS paid_users
  FROM {{ ref('stg_events') }}
  WHERE user_id IS NOT NULL  -- Exclude guest users
  
  {% if is_incremental() %}
    AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
  
  GROUP BY event_date
),

daily_metrics AS (
  SELECT
    COALESCE(sp.metric_date, s.metric_date, u.metric_date) AS metric_date,
    
    -- Song play metrics
    COALESCE(sp.total_songs_played, 0) AS total_songs_played,
    COALESCE(sp.unique_songs_played, 0) AS unique_songs_played,
    COALESCE(sp.unique_artists_played, 0) AS unique_artists_played,
    COALESCE(sp.total_skips, 0) AS total_skips,
    COALESCE(sp.total_complete_listens, 0) AS total_complete_listens,
    sp.avg_song_duration AS avg_song_duration_seconds,
    
    -- Session metrics
    COALESCE(s.total_sessions, 0) AS total_sessions,
    COALESCE(s.sessions_with_music, 0) AS sessions_with_music,
    COALESCE(s.long_sessions, 0) AS long_sessions,
    s.avg_session_duration_minutes,
    s.avg_songs_per_session,
    s.avg_engagement_score,
    COALESCE(s.total_events, 0) AS total_events,
    
    -- User metrics
    GREATEST(
      COALESCE(sp.active_users_songs, 0),
      COALESCE(s.active_users_sessions, 0)
    ) AS active_users,
    COALESCE(u.total_registered_users, 0) AS registered_users,
    COALESCE(u.free_users, 0) AS free_users,
    COALESCE(u.paid_users, 0) AS paid_users,
    
    -- Device distribution (songs)
    COALESCE(sp.plays_mobile, 0) AS plays_mobile,
    COALESCE(sp.plays_desktop, 0) AS plays_desktop,
    COALESCE(sp.plays_tablet, 0) AS plays_tablet,
    
    -- Device distribution (sessions)
    COALESCE(s.sessions_mobile, 0) AS sessions_mobile,
    COALESCE(s.sessions_desktop, 0) AS sessions_desktop,
    COALESCE(s.sessions_tablet, 0) AS sessions_tablet,
    
    -- Calculated KPIs
    CASE
      WHEN COALESCE(sp.total_songs_played, 0) > 0
      THEN ROUND(COALESCE(sp.total_skips, 0) * 100.0 / sp.total_songs_played, 2)
      ELSE 0
    END AS skip_rate_pct,
    
    CASE
      WHEN COALESCE(sp.total_songs_played, 0) > 0
      THEN ROUND(COALESCE(sp.total_complete_listens, 0) * 100.0 / sp.total_songs_played, 2)
      ELSE 0
    END AS completion_rate_pct,
    
    CASE
      WHEN COALESCE(s.total_sessions, 0) > 0
      THEN ROUND(COALESCE(s.sessions_with_music, 0) * 100.0 / s.total_sessions, 2)
      ELSE 0
    END AS music_session_rate_pct,
    
    CASE
      WHEN COALESCE(u.total_registered_users, 0) > 0
      THEN ROUND(COALESCE(u.paid_users, 0) * 100.0 / u.total_registered_users, 2)
      ELSE 0
    END AS paid_conversion_rate_pct,
    
    -- Day of week
    EXTRACT(DAYOFWEEK FROM COALESCE(sp.metric_date, s.metric_date, u.metric_date)) AS day_of_week,
    
    CASE
      WHEN EXTRACT(DAYOFWEEK FROM COALESCE(sp.metric_date, s.metric_date, u.metric_date)) IN (1, 7)
      THEN TRUE ELSE FALSE
    END AS is_weekend,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM daily_song_plays sp
  FULL OUTER JOIN daily_sessions s ON sp.metric_date = s.metric_date
  FULL OUTER JOIN daily_users u ON COALESCE(sp.metric_date, s.metric_date) = u.metric_date
)

SELECT * FROM daily_metrics
