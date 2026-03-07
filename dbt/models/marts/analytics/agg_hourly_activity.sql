{{
  config(
    materialized='incremental',
    unique_key=['activity_date', 'activity_hour'],
    partition_by={
      'field': 'activity_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['activity_date', 'activity_hour'],
    incremental_strategy='insert_overwrite',
    tags=['marts', 'analytics', 'aggregation']
  )
}}

/*
  Analytics Aggregation: Hourly Activity
  
  Purpose:
    Hourly platform activity for identifying peak usage times and patterns.
    Useful for capacity planning and user behavior analysis.
  
  Grain:
    One row per date + hour combination
  
  Sources:
    - fct_song_plays
    - fct_sessions
  
  Metrics:
    - Active users per hour
    - Songs played per hour
    - Sessions started per hour
*/

WITH hourly_song_plays AS (
  SELECT
    event_date AS activity_date,
    event_hour AS activity_hour,
    
    COUNT(*) AS songs_played,
    COUNT(DISTINCT user_id) AS active_users,
    COUNT(DISTINCT session_id) AS active_sessions,
    COUNT(DISTINCT artist_name) AS unique_artists,
    COUNT(DISTINCT song_identifier) AS unique_songs,
    
    -- Engagement metrics
    AVG(song_duration_seconds) AS avg_song_duration,
    SUM(CASE WHEN likely_skip THEN 1 ELSE 0 END) AS skips,
    SUM(CASE WHEN likely_complete_listen THEN 1 ELSE 0 END) AS complete_listens,
    
    -- By subscription
    SUM(CASE WHEN subscription_level = 'free' THEN 1 ELSE 0 END) AS plays_free,
    SUM(CASE WHEN subscription_level = 'paid' THEN 1 ELSE 0 END) AS plays_paid,
    COUNT(DISTINCT CASE WHEN subscription_level = 'free' THEN user_id END) AS active_free_users,
    COUNT(DISTINCT CASE WHEN subscription_level = 'paid' THEN user_id END) AS active_paid_users,
    
    -- By device
    SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) AS plays_mobile,
    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) AS plays_desktop,
    SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS plays_tablet,
    
    -- Weekend flag
    MAX(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS is_weekend
    
  FROM {{ ref('fct_song_plays') }}
  
  {% if is_incremental() %}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
  
  GROUP BY event_date, event_hour
),

hourly_sessions AS (
  SELECT
    session_date AS activity_date,
    session_start_hour AS activity_hour,
    
    COUNT(*) AS sessions_started,
    COUNT(DISTINCT user_id) AS users_starting_sessions,
    AVG(session_duration_minutes) AS avg_session_duration,
    AVG(songs_played) AS avg_songs_per_session,
    SUM(songs_played) AS total_songs_in_sessions,
    
    -- Session quality
    SUM(CASE WHEN has_song_plays THEN 1 ELSE 0 END) AS sessions_with_music,
    SUM(CASE WHEN is_long_session THEN 1 ELSE 0 END) AS long_sessions,
    AVG(engagement_score) AS avg_engagement_score
    
  FROM {{ ref('fct_sessions') }}
  
  {% if is_incremental() %}
    WHERE session_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
  
  GROUP BY session_date, session_start_hour
),

hourly_activity AS (
  SELECT
    COALESCE(sp.activity_date, s.activity_date) AS activity_date,
    COALESCE(sp.activity_hour, s.activity_hour) AS activity_hour,
    
    -- Song play metrics
    COALESCE(sp.songs_played, 0) AS songs_played,
    COALESCE(sp.unique_songs, 0) AS unique_songs,
    COALESCE(sp.unique_artists, 0) AS unique_artists,
    COALESCE(sp.skips, 0) AS skips,
    COALESCE(sp.complete_listens, 0) AS complete_listens,
    sp.avg_song_duration AS avg_song_duration_seconds,
    
    -- User metrics
    GREATEST(
      COALESCE(sp.active_users, 0),
      COALESCE(s.users_starting_sessions, 0)
    ) AS active_users,
    COALESCE(sp.active_free_users, 0) AS active_free_users,
    COALESCE(sp.active_paid_users, 0) AS active_paid_users,
    
    -- Session metrics
    COALESCE(s.sessions_started, 0) AS sessions_started,
    COALESCE(s.sessions_with_music, 0) AS sessions_with_music,
    COALESCE(s.long_sessions, 0) AS long_sessions,
    s.avg_session_duration AS avg_session_duration_minutes,
    s.avg_songs_per_session,
    s.avg_engagement_score,
    
    -- Device breakdown
    COALESCE(sp.plays_mobile, 0) AS plays_mobile,
    COALESCE(sp.plays_desktop, 0) AS plays_desktop,
    COALESCE(sp.plays_tablet, 0) AS plays_tablet,
    
    -- Subscription breakdown
    COALESCE(sp.plays_free, 0) AS plays_free,
    COALESCE(sp.plays_paid, 0) AS plays_paid,
    
    -- Calculated metrics
    CASE
      WHEN COALESCE(sp.songs_played, 0) > 0
      THEN ROUND(COALESCE(sp.skips, 0) * 100.0 / sp.songs_played, 2)
      ELSE 0
    END AS skip_rate_pct,
    
    CASE
      WHEN GREATEST(COALESCE(sp.active_users, 0), COALESCE(s.users_starting_sessions, 0)) > 0
      THEN ROUND(
        COALESCE(sp.songs_played, 0) * 1.0 / 
        GREATEST(COALESCE(sp.active_users, 0), COALESCE(s.users_starting_sessions, 0)),
        2
      )
      ELSE 0
    END AS songs_per_active_user,
    
    -- Time classification
    CASE
      WHEN COALESCE(sp.activity_hour, s.activity_hour) BETWEEN 6 AND 11 THEN 'morning'
      WHEN COALESCE(sp.activity_hour, s.activity_hour) BETWEEN 12 AND 17 THEN 'afternoon'
      WHEN COALESCE(sp.activity_hour, s.activity_hour) BETWEEN 18 AND 22 THEN 'evening'
      ELSE 'night'
    END AS time_of_day,
    
    -- Day of week
    EXTRACT(DAYOFWEEK FROM COALESCE(sp.activity_date, s.activity_date)) AS day_of_week,
    COALESCE(sp.is_weekend, 0) AS is_weekend,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM hourly_song_plays sp
  FULL OUTER JOIN hourly_sessions s 
    ON sp.activity_date = s.activity_date 
    AND sp.activity_hour = s.activity_hour
)

SELECT * FROM hourly_activity
