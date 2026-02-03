{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

/*
  Dimension Table: Songs
  
  Purpose:
    Dimension table for unique songs and artists.
    Includes aggregated metrics about song popularity.
  
  Grain:
    One row per unique song (artist + song combination)
  
  Source:
    stg_song_plays
  
  Update Strategy:
    Full refresh - recalculates song statistics
*/

WITH song_plays AS (
  SELECT
    artist_name,
    song_title,
    song_duration_seconds,
    song_identifier,
    user_id,
    event_date
  FROM {{ ref('stg_song_plays') }}
),

song_stats AS (
  SELECT
    artist_name,
    song_title,
    song_identifier,
    
    -- Take most common duration (in case of slight variations)
    APPROX_TOP_COUNT(song_duration_seconds, 1)[OFFSET(0)].value AS song_duration_seconds,
    
    -- Play statistics
    COUNT(*) AS total_plays,
    COUNT(DISTINCT user_id) AS unique_listeners,
    COUNT(DISTINCT event_date) AS days_played,
    
    -- First and last play
    MIN(event_date) AS first_played_date,
    MAX(event_date) AS last_played_date
    
  FROM song_plays
  GROUP BY artist_name, song_title, song_identifier
),

song_dimension AS (
  SELECT
    -- Create surrogate key
    {{ dbt_utils.generate_surrogate_key(['artist_name', 'song_title']) }} AS song_key,
    
    -- Natural key
    song_identifier,
    
    -- Song attributes
    artist_name,
    song_title,
    song_duration_seconds,
    ROUND(song_duration_seconds / 60.0, 2) AS song_duration_minutes,
    
    -- Song length category
    CASE
      WHEN song_duration_seconds < 60 THEN 'very_short'
      WHEN song_duration_seconds < 180 THEN 'short'
      WHEN song_duration_seconds < 300 THEN 'medium'
      WHEN song_duration_seconds < 600 THEN 'long'
      ELSE 'very_long'
    END AS song_length_category,
    
    -- Popularity metrics
    total_plays,
    unique_listeners,
    days_played,
    
    -- Engagement metric (plays per unique listener)
    ROUND(total_plays / NULLIF(unique_listeners, 0), 2) AS avg_plays_per_listener,
    
    -- Dates
    first_played_date,
    last_played_date,
    DATE_DIFF(last_played_date, first_played_date, DAY) + 1 AS days_in_catalog,
    
    -- Popularity tier
    CASE
      WHEN total_plays >= 1000 THEN 'very_popular'
      WHEN total_plays >= 500 THEN 'popular'
      WHEN total_plays >= 100 THEN 'moderate'
      WHEN total_plays >= 10 THEN 'niche'
      ELSE 'rare'
    END AS popularity_tier,
    
    -- Recency flag
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), last_played_date, DAY) <= 7 THEN TRUE
      ELSE FALSE
    END AS played_in_last_week,
    
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), last_played_date, DAY) <= 30 THEN TRUE
      ELSE FALSE
    END AS played_in_last_month,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM song_stats
)

SELECT * FROM song_dimension
ORDER BY total_plays DESC
