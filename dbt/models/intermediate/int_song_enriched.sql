{{
  config(
    materialized='ephemeral',
    tags=['intermediate', 'song_plays']
  )
}}

/*
  Intermediate Model: Enriched Song Plays
  
  Purpose:
    Enrich song play events with additional context and metrics.
    Add song popularity, user listening patterns, etc.
  
  Source:
    stg_song_plays
  
  Enrichments:
    - Song play sequence within session
    - Time since previous song in session
    - Running totals of plays by song/artist
*/

WITH song_plays_with_sequence AS (
  SELECT
    *,
    
    -- Song play sequence in session
    ROW_NUMBER() OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS play_sequence_in_session,
    
    -- Time since previous song in session (seconds)
    TIMESTAMP_DIFF(
      event_timestamp,
      LAG(event_timestamp) OVER (PARTITION BY session_id ORDER BY event_timestamp),
      SECOND
    ) AS seconds_since_previous_song,
    
    -- Previous song info (for skip detection)
    LAG(song_title) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS previous_song_title,
    
    LAG(artist_name) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) AS previous_artist_name,
    
    -- Is this the first song in session?
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) = 1 THEN TRUE
      ELSE FALSE
    END AS is_first_song_in_session,
    
    -- Is this the last song in session?
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp DESC) = 1 THEN TRUE
      ELSE FALSE
    END AS is_last_song_in_session
    
  FROM {{ ref('stg_song_plays') }}
),

enriched AS (
  SELECT
    *,
    
    -- Skip detection (if previous song played for less than 30 seconds)
    CASE
      WHEN seconds_since_previous_song IS NOT NULL 
        AND seconds_since_previous_song < 30 
        AND NOT is_first_song_in_session
      THEN TRUE
      ELSE FALSE
    END AS likely_skip,
    
    -- Complete listen detection (if song duration is close to time since previous)
    CASE
      WHEN seconds_since_previous_song IS NOT NULL
        AND LAG(song_duration_seconds) OVER (PARTITION BY session_id ORDER BY event_timestamp) IS NOT NULL
        AND ABS(
          seconds_since_previous_song - 
          LAG(song_duration_seconds) OVER (PARTITION BY session_id ORDER BY event_timestamp)
        ) < 10  -- Within 10 seconds
      THEN TRUE
      ELSE FALSE
    END AS likely_complete_listen,
    
    -- Listening time classification
    CASE
      WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 6 AND 11 THEN 'morning'
      WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 12 AND 17 THEN 'afternoon'
      WHEN EXTRACT(HOUR FROM event_timestamp) BETWEEN 18 AND 22 THEN 'evening'
      ELSE 'night'
    END AS listening_time_of_day
    
  FROM song_plays_with_sequence
)

SELECT * FROM enriched
