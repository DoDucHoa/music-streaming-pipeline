{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

/*
  Dimension Table: Users
  
  Purpose:
    Slowly Changing Dimension (Type 1) for user information.
    Maintains latest state of each user.
  
  Grain:
    One row per user (user_id)
  
  Source:
    stg_events (deduplicated to get latest user state)
  
  Update Strategy:
    Full refresh - captures latest user attributes
*/

WITH user_events AS (
  SELECT
    user_id,
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    city,
    state,
    zip_code,
    registration_timestamp,
    event_timestamp,
    
    -- Get latest record for each user
    ROW_NUMBER() OVER (
      PARTITION BY user_id 
      ORDER BY event_timestamp DESC
    ) AS row_num
    
  FROM {{ ref('stg_events') }}
  WHERE user_id IS NOT NULL  -- Exclude guest users
),

latest_user_state AS (
  SELECT
    user_id,
    first_name,
    last_name,
    gender,
    subscription_level,
    auth_status,
    city,
    state,
    zip_code,
    registration_timestamp,
    event_timestamp AS last_activity_timestamp
    
  FROM user_events
  WHERE row_num = 1
),

user_stats AS (
  -- Calculate user-level statistics
  SELECT
    user_id,
    MIN(event_date) AS first_seen_date,
    MAX(event_date) AS last_seen_date,
    COUNT(DISTINCT event_date) AS days_active,
    COUNT(*) AS total_events,
    SUM(CASE WHEN page_name = 'NextSong' THEN 1 ELSE 0 END) AS total_songs_played
    
  FROM {{ ref('stg_events') }}
  WHERE user_id IS NOT NULL
  GROUP BY user_id
),

user_dimension AS (
  SELECT
    -- Primary key
    u.user_id,
    
    -- User attributes
    u.first_name,
    u.last_name,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.gender,
    u.subscription_level,
    u.auth_status,
    
    -- Location
    u.city,
    u.state,
    u.zip_code,
    CONCAT(u.city, ', ', u.state) AS location_display,
    
    -- Important dates
    u.registration_timestamp,
    DATE(u.registration_timestamp) AS registration_date,
    u.last_activity_timestamp,
    DATE(u.last_activity_timestamp) AS last_activity_date,
    
    -- Activity metrics
    s.first_seen_date,
    s.last_seen_date,
    s.days_active,
    s.total_events,
    s.total_songs_played,
    
    -- Derived metrics
    DATE_DIFF(s.last_seen_date, s.first_seen_date, DAY) + 1 AS lifetime_days,
    
    CASE
      WHEN s.days_active >= 30 THEN 'power_user'
      WHEN s.days_active >= 10 THEN 'regular_user'
      WHEN s.days_active >= 3 THEN 'casual_user'
      ELSE 'new_user'
    END AS user_segment,
    
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), s.last_seen_date, DAY) > 30 THEN TRUE
      ELSE FALSE
    END AS is_inactive,
    
    CASE
      WHEN u.subscription_level = 'paid' THEN TRUE
      ELSE FALSE
    END AS is_paid_subscriber,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
  FROM latest_user_state u
  LEFT JOIN user_stats s ON u.user_id = s.user_id
)

SELECT * FROM user_dimension
