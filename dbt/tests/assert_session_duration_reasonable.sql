-- Test: Check that session duration is logical
-- Session should not be longer than 24 hours (86400 seconds)

SELECT
    session_id,
    session_duration_seconds,
    session_start_timestamp,
    session_end_timestamp
FROM {{ ref('fct_sessions') }}
WHERE session_duration_seconds > 86400
