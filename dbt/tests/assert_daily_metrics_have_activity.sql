-- Test: Check that daily metrics have at least some activity
-- If a date exists, it should have at least 1 song played or 1 active user

SELECT
    metric_date
FROM {{ ref('agg_daily_metrics') }}
WHERE 
    total_songs_played = 0 
    AND active_users = 0
    AND total_sessions = 0
