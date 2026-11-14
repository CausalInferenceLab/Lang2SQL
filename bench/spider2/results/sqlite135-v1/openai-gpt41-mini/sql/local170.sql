WITH
  base_data AS (
    SELECT 
      lt.state AS state_abbrev,
      l.gender,
      l.id_bioguide AS legislator_id,
      MIN(date(lt.term_start)) AS first_term_start
    FROM legislators_terms lt
    JOIN legislators l ON lt.id_bioguide = l.id_bioguide
    GROUP BY lt.state, l.gender, l.id_bioguide
  ),
  retention_years AS (
    SELECT 0 AS years_after UNION ALL
    SELECT 2 UNION ALL
    SELECT 4 UNION ALL
    SELECT 6 UNION ALL
    SELECT 8 UNION ALL
    SELECT 10
  ),
  retained_counts AS (
    SELECT
      bd.state_abbrev,
      bd.gender,
      ry.years_after,
      COUNT(DISTINCT bd.legislator_id) AS cohort_size,
      SUM(
        EXISTS (
          SELECT 1
          FROM legislators_terms lt
          WHERE lt.id_bioguide = bd.legislator_id
            AND date(lt.term_start) <= date(bd.first_term_start, '+' || ry.years_after || ' years', 'start of year', '+11 months', '+30 days')
            AND date(lt.term_end, 'start of day', '+1 day', '-1 second') >= date(bd.first_term_start, '+' || ry.years_after || ' years', 'start of year', '+11 months', '+30 days')
        )
      ) AS retained_count
    FROM base_data bd
    CROSS JOIN retention_years ry
    GROUP BY bd.state_abbrev, bd.gender, ry.years_after
  ),
  retention_rates AS (
    SELECT
      state_abbrev,
      gender,
      years_after,
      CAST(retained_count AS FLOAT) / cohort_size AS retention_rate
    FROM retained_counts
  ),
  retention_filtered AS (
    SELECT state_abbrev, gender
    FROM retention_rates
    WHERE retention_rate > 0
    GROUP BY state_abbrev, gender
    HAVING COUNT(*) = 6
  ),
  states_with_both_genders AS (
    SELECT state_abbrev
    FROM retention_filtered
    GROUP BY state_abbrev
    HAVING COUNT(DISTINCT gender) = 2
  )
SELECT state_abbrev
FROM states_with_both_genders
ORDER BY state_abbrev;
