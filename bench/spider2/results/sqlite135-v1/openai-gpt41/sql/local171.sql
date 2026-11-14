WITH male_la_legislators AS (
    SELECT l.id, l.gender, l.state, MIN(lt.start_date) AS first_term_start
    FROM legislators l
    JOIN legislators_terms lt ON l.id = lt.legislator_id
    WHERE l.gender = 'M' AND l.state = 'LA'
    GROUP BY l.id
),
years_elapsed AS (
    SELECT 
        mll.id,
        mll.first_term_start,
        (CAST(strftime('%Y', lt.start_date) AS INTEGER) - CAST(strftime('%Y', mll.first_term_start) AS INTEGER)) AS years_since_first_term,
        lt.start_date,
        lt.end_date
    FROM male_la_legislators mll
    JOIN legislators_terms lt ON mll.id = lt.legislator_id
),
agg_years AS (
    -- Build candidate (legislator_id, years_elapsed) for relevant years and Dec 31 of year in any of their terms
    SELECT
        ye.id,
        ye.first_term_start,
        y AS years_elapsed,
        date(substr(ye.first_term_start, 1, 4) || '-12-31', '+' || y || ' years') AS dec31_date
    FROM years_elapsed ye
    CROSS JOIN (
        -- Generate years_elapsed from 31 to 49 (i.e., >30 and <50)
        SELECT  y FROM (SELECT 31 AS y UNION SELECT 32 UNION SELECT 33 UNION SELECT 34 UNION SELECT 35 UNION 
                             SELECT 36 UNION SELECT 37 UNION SELECT 38 UNION SELECT 39 UNION SELECT 40 UNION
                             SELECT 41 UNION SELECT 42 UNION SELECT 43 UNION SELECT 44 UNION SELECT 45 UNION
                             SELECT 46 UNION SELECT 47 UNION SELECT 48 UNION SELECT 49)
    )
),
active_at_dec31 AS (
    SELECT
        ay.years_elapsed,
        ay.id
    FROM agg_years ay
    JOIN legislators_terms t
      ON t.legislator_id = ay.id
     AND date(ay.dec31_date) BETWEEN t.start_date AND t.end_date
    GROUP BY ay.years_elapsed, ay.id
)
SELECT
    years_elapsed,
    COUNT(DISTINCT id) AS num_active_legislators
FROM active_at_dec31
GROUP BY years_elapsed
ORDER BY years_elapsed;
