{{
  config(
    materialized='view',
    tags=['staging', 'song_plays']
  )
}}

/*
  Staging Model: Song Plays
  
  Purpose:
    Extract only song play events (NextSong page) with complete song information.
    This is used for music-specific analytics.
  
  Source:
    stg_events
  
  Filters:
    - Only NextSong page events
    - Must have artist and song information
    - Valid song duration (> 0 seconds)
*/

WITH song_plays AS (
  SELECT
    -- Event identifiers
    event_id,
    session_id,
    
    -- User information
    user_id,
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    
    -- Song information
    artist_name,
    song_title,
    song_duration_seconds,
    
    -- Create composite song identifier (hash of artist + song)
    TO_HEX(MD5(CONCAT(COALESCE(artist_name, 'Unknown'), '|', COALESCE(song_title, 'Unknown')))) AS song_identifier,
    
    -- Derived song metrics
    ROUND(song_duration_seconds / 60.0, 2) AS song_duration_minutes,
    CASE
      WHEN song_duration_seconds < 60 THEN 'very_short'
      WHEN song_duration_seconds < 180 THEN 'short'
      WHEN song_duration_seconds < 300 THEN 'medium'
      WHEN song_duration_seconds < 600 THEN 'long'
      ELSE 'very_long'
    END AS song_length_category,
    
    -- Location & Device
    city,
    state,
    location_full,
    zip_code,
    latitude,
    longitude,
    user_agent,
    
    -- Device classification (simple)
    CASE
      WHEN LOWER(user_agent) LIKE '%mobile%' OR LOWER(user_agent) LIKE '%android%' THEN 'mobile'
      WHEN LOWER(user_agent) LIKE '%tablet%' OR LOWER(user_agent) LIKE '%ipad%' THEN 'tablet'
      WHEN LOWER(user_agent) LIKE '%windows%' OR LOWER(user_agent) LIKE '%macintosh%' OR LOWER(user_agent) LIKE '%linux%' THEN 'desktop'
      ELSE 'other'
    END AS device_type,
    
    -- Browser detection
    CASE
      WHEN LOWER(user_agent) LIKE '%chrome%' THEN 'Chrome'
      WHEN LOWER(user_agent) LIKE '%firefox%' THEN 'Firefox'
      WHEN LOWER(user_agent) LIKE '%safari%' THEN 'Safari'
      WHEN LOWER(user_agent) LIKE '%edge%' THEN 'Edge'
      ELSE 'Other'
    END AS browser,
    
    -- Timestamp fields
    event_timestamp,
    event_date,
    event_hour,
    day_of_week,
    
    -- Time of day classification
    CASE
      WHEN event_hour BETWEEN 6 AND 11 THEN 'morning'
      WHEN event_hour BETWEEN 12 AND 17 THEN 'afternoon'
      WHEN event_hour BETWEEN 18 AND 22 THEN 'evening'
      ELSE 'night'
    END AS time_of_day,
    
    -- Weekend flag
    CASE
      WHEN day_of_week IN (1, 7) THEN TRUE  -- Sunday=1, Saturday=7
      ELSE FALSE
    END AS is_weekend,
    
    -- Session context
    item_in_session,
    
    -- Processing metadata
    spark_processed_at,
    spark_processing_date
    
  FROM {{ ref('stg_events') }}
  
  WHERE
    page_name = 'NextSong'
    AND artist_name IS NOT NULL
    AND song_title IS NOT NULL
    AND song_duration_seconds > 0
    AND song_duration_seconds < 7200  -- Filter out unreasonably long songs (> 2 hours)
)

SELECT * FROM song_plays
