WITH years_offset AS (
  SELECT 0 AS offset UNION ALL
  SELECT 2 UNION ALL
  SELECT 4 UNION ALL
  SELECT 6 UNION ALL
  SELECT 8 UNION ALL
  SELECT 10
),
first_terms AS (
  SELECT
    l.id AS legislator_id,
    l.gender,
    lt.state,
    MIN(lt.start_date) AS first_term_start
  FROM legislators l
  JOIN legislators_terms lt ON l.id = lt.legislator_id
  GROUP BY l.id, l.gender, lt.state
),
survival_points AS (
  SELECT
    ft.legislator_id,
    ft.gender,
    ft.state,
    y.offset,
    DATE(ft.first_term_start, '+' || y.offset || ' years') AS check_date,
    strftime('%Y', DATE(ft.first_term_start, '+' || y.offset || ' years')) || '-12-31' AS check_end_of_year
  FROM first_terms ft
  CROSS JOIN years_offset y
),
term_spans AS (
  SELECT
    lt.legislator_id,
    lt.state,
    lt.start_date,
    IFNULL(lt.end_date, '2999-12-31') AS end_date
  FROM legislators_terms lt
),
retention_checks AS (
  SELECT
    sp.legislator_id,
    sp.gender,
    sp.state,
    sp.offset,
    sp.check_end_of_year,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM term_spans t
        WHERE t.legislator_id = sp.legislator_id
          AND t.state = sp.state
          AND date(sp.check_end_of_year) BETWEEN date(t.start_date) AND date(t.end_date)
      ) THEN 1
      ELSE 0
    END AS retained
  FROM survival_points sp
),
retention_rates AS (
  SELECT
    state,
    gender,
    offset,
    SUM(retained) * 1.0 / COUNT(*) AS retention_rate
  FROM retention_checks
  GROUP BY state, gender, offset
),
valid_states AS (
  SELECT
    state
  FROM retention_rates
  WHERE retention_rate > 0
  GROUP BY state, gender
  HAVING COUNT(CASE WHEN offset IN (0,2,4,6,8,10) THEN 1 END) = 6
),
states_with_both_genders AS (
  SELECT state
  FROM valid_states v
  JOIN retention_rates rr_m ON v.state = rr_m.state AND rr_m.gender = 'M' AND rr_m.retention_rate > 0 AND rr_m.offset IN (0,2,4,6,8,10)
  JOIN retention_rates rr_f ON v.state = rr_f.state AND rr_f.gender = 'F' AND rr_f.retention_rate > 0 AND rr_f.offset IN (0,2,4,6,8,10)
  GROUP BY v.state
  HAVING COUNT(DISTINCT rr_m.gender) = 1 AND COUNT(DISTINCT rr_f.gender) = 1
)
SELECT DISTINCT state
FROM states_with_both_genders
ORDER BY state;
