{{
  config(
    materialized='incremental',
    unique_key='event_id',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'day'
    },
    cluster_by=['user_id', 'artist_name', 'song_title'],
    incremental_strategy='insert_overwrite',
    tags=['marts', 'core', 'facts']
  )
}}

/*
  Fact Table: Song Plays
  
  Purpose:
    Core fact table for song play analytics. One row per song play event
    with all relevant dimensions and metrics.
  
  Grain:
    One row per song play event (event_id)
  
  Source:
    int_song_enriched (ephemeral intermediate model)
  
  Incremental Strategy:
    - Partition by event_date
    - Cluster by user_id, artist_name, song_title
    - Reprocesses last 3 days on each run (lookback window)
    - Uses insert_overwrite to handle late-arriving data
*/

WITH song_plays AS (
  SELECT
    -- Primary key
    event_id,
    
    -- Foreign keys
    session_id,
    user_id,
    
    -- User attributes (denormalized for query performance)
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    
    -- Song attributes
    artist_name,
    song_title,
    song_identifier,
    song_duration_seconds,
    song_duration_minutes,
    song_length_category,
    
    -- Location dimensions
    city,
    state,
    location_full,
    zip_code,
    latitude,
    longitude,
    
    -- Device dimensions
    user_agent,
    device_type,
    browser,
    
    -- Time dimensions
    event_timestamp,
    event_date,
    event_hour,
    day_of_week,
    time_of_day,
    is_weekend,
    
    -- Session context
    item_in_session,
    play_sequence_in_session,
    is_first_song_in_session,
    is_last_song_in_session,
    
    -- Listening behavior metrics
    seconds_since_previous_song,
    likely_skip,
    likely_complete_listen,
    
    -- Previous song context (for analysis)
    previous_song_title,
    previous_artist_name,
    
    -- Metadata
    spark_processed_at,
    spark_processing_date,
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM {{ ref('int_song_enriched') }}
  
  {% if is_incremental() %}
    -- Incremental: only process last N days (lookback window)
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('incremental_lookback_days', 3) }} DAY)
  {% endif %}
)

SELECT * FROM song_plays
