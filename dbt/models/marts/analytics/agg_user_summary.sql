{{
  config(
    materialized='table',
    tags=['marts', 'analytics', 'aggregation']
  )
}}

/*
  Analytics Aggregation: User Summary
  
  Purpose:
    Per-user lifetime metrics and favorite songs/artists.
    Used for user segmentation, personalization, and retention analysis.
  
  Grain:
    One row per user
  
  Sources:
    - fct_song_plays
    - fct_sessions
    - dim_users
  
  Metrics:
    - Total plays, sessions, listening time
    - Favorite artists and songs
    - User behavior patterns
*/

WITH user_song_plays AS (
  SELECT
    user_id,
    COUNT(*) AS total_songs_played,
    COUNT(DISTINCT song_identifier) AS unique_songs_played,
    COUNT(DISTINCT artist_name) AS unique_artists_listened,
    COUNT(DISTINCT event_date) AS days_with_plays,
    SUM(song_duration_seconds) AS total_listening_seconds,
    AVG(song_duration_seconds) AS avg_song_duration,
    
    -- First and last plays
    MIN(event_date) AS first_play_date,
    MAX(event_date) AS last_play_date,
    
    -- Engagement
    SUM(CASE WHEN likely_skip THEN 1 ELSE 0 END) AS total_skips,
    SUM(CASE WHEN likely_complete_listen THEN 1 ELSE 0 END) AS total_complete_listens,
    
    -- Device preferences
    SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) AS plays_mobile,
    SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) AS plays_desktop,
    SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS plays_tablet,
    
    -- Time preferences
    SUM(CASE WHEN time_of_day = 'morning' THEN 1 ELSE 0 END) AS plays_morning,
    SUM(CASE WHEN time_of_day = 'afternoon' THEN 1 ELSE 0 END) AS plays_afternoon,
    SUM(CASE WHEN time_of_day = 'evening' THEN 1 ELSE 0 END) AS plays_evening,
    SUM(CASE WHEN time_of_day = 'night' THEN 1 ELSE 0 END) AS plays_night,
    SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS plays_weekend,
    SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) AS plays_weekday
    
  FROM {{ ref('fct_song_plays') }}
  WHERE user_id IS NOT NULL
  GROUP BY user_id
),

user_sessions AS (
  SELECT
    user_id,
    COUNT(*) AS total_sessions,
    AVG(session_duration_minutes) AS avg_session_duration_minutes,
    SUM(session_duration_minutes) AS total_session_minutes,
    AVG(songs_played) AS avg_songs_per_session,
    AVG(engagement_score) AS avg_engagement_score,
    
    -- Session quality
    SUM(CASE WHEN has_song_plays THEN 1 ELSE 0 END) AS sessions_with_music,
    SUM(CASE WHEN is_long_session THEN 1 ELSE 0 END) AS long_sessions,
    
    -- First and last sessions
    MIN(session_date) AS first_session_date,
    MAX(session_date) AS last_session_date
    
  FROM {{ ref('fct_sessions') }}
  WHERE user_id IS NOT NULL
  GROUP BY user_id
),

favorite_artists AS (
  SELECT
    user_id,
    artist_name,
    COUNT(*) AS plays,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS artist_rank
  FROM {{ ref('fct_song_plays') }}
  WHERE user_id IS NOT NULL
  GROUP BY user_id, artist_name
),

favorite_songs AS (
  SELECT
    user_id,
    song_identifier,
    artist_name,
    song_title,
    COUNT(*) AS plays,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS song_rank
  FROM {{ ref('fct_song_plays') }}
  WHERE user_id IS NOT NULL
  GROUP BY user_id, song_identifier, artist_name, song_title
),

user_summary AS (
  SELECT
    u.user_id,
    
    -- User attributes
    u.full_name,
    u.gender,
    u.subscription_level,
    u.city,
    u.state,
    u.user_segment,
    u.is_inactive,
    
    -- Lifetime dates
    u.registration_date,
    COALESCE(sp.first_play_date, s.first_session_date) AS first_activity_date,
    COALESCE(sp.last_play_date, s.last_session_date) AS last_activity_date,
    u.days_active,
    
    -- Song play metrics
    COALESCE(sp.total_songs_played, 0) AS total_songs_played,
    COALESCE(sp.unique_songs_played, 0) AS unique_songs_played,
    COALESCE(sp.unique_artists_listened, 0) AS unique_artists_listened,
    COALESCE(sp.days_with_plays, 0) AS days_with_plays,
    
    -- Listening time
    COALESCE(sp.total_listening_seconds, 0) AS total_listening_seconds,
    ROUND(COALESCE(sp.total_listening_seconds, 0) / 60.0, 2) AS total_listening_minutes,
    ROUND(COALESCE(sp.total_listening_seconds, 0) / 3600.0, 2) AS total_listening_hours,
    sp.avg_song_duration AS avg_song_duration_seconds,
    
    -- Session metrics
    COALESCE(s.total_sessions, 0) AS total_sessions,
    COALESCE(s.sessions_with_music, 0) AS sessions_with_music,
    COALESCE(s.long_sessions, 0) AS long_sessions,
    s.avg_session_duration_minutes,
    s.avg_songs_per_session,
    s.avg_engagement_score,
    
    -- Engagement rates
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
    
    -- Device preferences
    CASE
      WHEN sp.plays_mobile >= sp.plays_desktop AND sp.plays_mobile >= COALESCE(sp.plays_tablet, 0)
        THEN 'mobile'
      WHEN sp.plays_desktop >= COALESCE(sp.plays_tablet, 0)
        THEN 'desktop'
      ELSE 'tablet'
    END AS preferred_device,
    
    -- Time preferences
    CASE
      WHEN sp.plays_morning = GREATEST(sp.plays_morning, sp.plays_afternoon, sp.plays_evening, sp.plays_night)
        THEN 'morning'
      WHEN sp.plays_afternoon = GREATEST(sp.plays_afternoon, sp.plays_evening, sp.plays_night)
        THEN 'afternoon'
      WHEN sp.plays_evening = GREATEST(sp.plays_evening, sp.plays_night)
        THEN 'evening'
      ELSE 'night'
    END AS preferred_time_of_day,
    
    CASE
      WHEN COALESCE(sp.plays_weekend, 0) > COALESCE(sp.plays_weekday, 0)
        THEN 'weekend_listener'
      ELSE 'weekday_listener'
    END AS listening_pattern,
    
    -- Favorite artist (top 1)
    fa1.artist_name AS favorite_artist_1,
    fa1.plays AS favorite_artist_1_plays,
    
    -- Favorite song (top 1)
    fs1.song_identifier AS favorite_song_1,
    fs1.artist_name AS favorite_song_1_artist,
    fs1.song_title AS favorite_song_1_title,
    fs1.plays AS favorite_song_1_plays,
    
    -- Calculated scores
    CASE
      WHEN COALESCE(u.days_active, 0) > 0
      THEN ROUND(COALESCE(sp.total_songs_played, 0) * 1.0 / u.days_active, 2)
      ELSE 0
    END AS avg_songs_per_active_day,
    
    -- User lifetime value proxy (simple calculation)
    (COALESCE(sp.total_songs_played, 0) * 0.001) + 
    (COALESCE(s.total_sessions, 0) * 0.01) +
    (CASE WHEN u.subscription_level = 'paid' THEN 10 ELSE 0 END) AS ltv_score,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM {{ ref('dim_users') }} u
  LEFT JOIN user_song_plays sp ON u.user_id = sp.user_id
  LEFT JOIN user_sessions s ON u.user_id = s.user_id
  LEFT JOIN favorite_artists fa1 ON u.user_id = fa1.user_id AND fa1.artist_rank = 1
  LEFT JOIN favorite_songs fs1 ON u.user_id = fs1.user_id AND fs1.song_rank = 1
)

SELECT * FROM user_summary
ORDER BY total_songs_played DESC
