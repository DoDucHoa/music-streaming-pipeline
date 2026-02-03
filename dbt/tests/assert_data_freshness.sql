-- Test: Check data freshness - ensure we have recent data
-- This test warns if the most recent data is older than 2 days

SELECT
    MAX(event_date) AS latest_date,
    CURRENT_DATE() AS today,
    DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) AS days_behind
FROM {{ ref('fct_song_plays') }}
HAVING DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) > 2
