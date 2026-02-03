{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'sessions']
  )
}}

/*
  Intermediate Model: User Sessions
  
  Purpose:
    Aggregate events by session to create session-level metrics.
    Uses existing session_id from eventsim.
  
  Source:
    stg_events
  
  Session Metrics:
    - Session duration
    - Number of events per session
    - Number of songs played
    - Pages visited
    - Session start/end times
*/

WITH session_events AS (
  SELECT
    session_id,
    user_id,
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    
    -- Location (take first non-null value in session)
    FIRST_VALUE(city IGNORE NULLS) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_city,
    
    FIRST_VALUE(state IGNORE NULLS) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_state,
    
    -- Device info (take first value)
    FIRST_VALUE(user_agent IGNORE NULLS) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_user_agent,
    
    -- Event details
    page_name,
    event_timestamp,
    event_date,
    
    -- Mark if event is a song play
    CASE WHEN page_name = 'NextSong' THEN 1 ELSE 0 END AS is_song_play,
    
    -- Session boundaries
    MIN(event_timestamp) OVER (PARTITION BY session_id) AS session_start,
    MAX(event_timestamp) OVER (PARTITION BY session_id) AS session_end,
    
    -- Count events
    COUNT(*) OVER (PARTITION BY session_id) AS total_events_in_session
    
  FROM {{ ref('stg_events') }}
  WHERE session_id IS NOT NULL
),

session_summary AS (
  SELECT
    session_id,
    
    -- User information (take any value - should be consistent within session)
    ANY_VALUE(user_id) AS user_id,
    ANY_VALUE(first_name) AS first_name,
    ANY_VALUE(last_name) AS last_name,
    ANY_VALUE(gender) AS gender,
    ANY_VALUE(subscription_level) AS subscription_level,
    ANY_VALUE(auth_status) AS auth_status,
    
    -- Session location
    ANY_VALUE(session_city) AS city,
    ANY_VALUE(session_state) AS state,
    ANY_VALUE(session_user_agent) AS user_agent,
    
    -- Device classification
    CASE
      WHEN LOWER(ANY_VALUE(session_user_agent)) LIKE '%mobile%' OR LOWER(ANY_VALUE(session_user_agent)) LIKE '%android%' THEN 'mobile'
      WHEN LOWER(ANY_VALUE(session_user_agent)) LIKE '%tablet%' OR LOWER(ANY_VALUE(session_user_agent)) LIKE '%ipad%' THEN 'tablet'
      WHEN LOWER(ANY_VALUE(session_user_agent)) LIKE '%windows%' OR LOWER(ANY_VALUE(session_user_agent)) LIKE '%macintosh%' THEN 'desktop'
      ELSE 'other'
    END AS device_type,
    
    -- Session timing
    MIN(session_start) AS session_start_timestamp,
    MAX(session_end) AS session_end_timestamp,
    DATE(MIN(session_start)) AS session_date,
    EXTRACT(HOUR FROM MIN(session_start)) AS session_start_hour,
    
    -- Session duration in seconds
    TIMESTAMP_DIFF(MAX(session_end), MIN(session_start), SECOND) AS session_duration_seconds,
    
    -- Session metrics
    COUNT(*) AS total_events,
    SUM(is_song_play) AS songs_played,
    COUNT(DISTINCT page_name) AS unique_pages_visited,
    
    -- Engagement flags
    CASE 
      WHEN SUM(is_song_play) > 0 THEN TRUE 
      ELSE FALSE 
    END AS has_song_plays,
    
    CASE 
      WHEN TIMESTAMP_DIFF(MAX(session_end), MIN(session_start), SECOND) > 300 THEN TRUE  -- > 5 minutes
      ELSE FALSE 
    END AS is_long_session,
    
    -- Page activity summary (JSON array of pages visited)
    STRING_AGG(DISTINCT page_name, ', ' ORDER BY page_name) AS pages_visited
    
  FROM session_events
  GROUP BY session_id
)

SELECT
  *,
  
  -- Derived metrics
  ROUND(session_duration_seconds / 60.0, 2) AS session_duration_minutes,
  
  -- Engagement score (simple calculation)
  -- Formula: (songs_played * 2) + (unique_pages_visited) + (long_session_bonus)
  (songs_played * 2) + unique_pages_visited + (CASE WHEN is_long_session THEN 5 ELSE 0 END) AS engagement_score,
  
  -- Session classification
  CASE
    WHEN session_duration_seconds < 60 THEN 'very_short'  -- < 1 minute
    WHEN session_duration_seconds < 300 THEN 'short'      -- < 5 minutes
    WHEN session_duration_seconds < 900 THEN 'medium'     -- < 15 minutes
    WHEN session_duration_seconds < 1800 THEN 'long'      -- < 30 minutes
    ELSE 'very_long'                                       -- >= 30 minutes
  END AS session_length_category

FROM session_summary
