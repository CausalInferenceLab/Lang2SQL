WITH session_pages AS (
    SELECT
        session_id,
        page,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp DESC) AS rn_desc
    FROM activity_log
),
landing_exit AS (
    SELECT
        session_id,
        CASE WHEN rn_asc = 1 THEN page END AS landing_page,
        CASE WHEN rn_desc = 1 THEN page END AS exit_page
    FROM session_pages
),
union_pages AS (
    SELECT session_id, landing_page AS page
    FROM landing_exit
    WHERE landing_page IS NOT NULL

    UNION

    SELECT session_id, exit_page AS page
    FROM landing_exit
    WHERE exit_page IS NOT NULL
)
SELECT
    page,
    COUNT(DISTINCT session_id) AS unique_session_count
FROM union_pages
GROUP BY page;
