SELECT
    d.A2 AS district_name,
    ROUND(100.0 * (d.A13 - d.A12) / NULLIF(d.A12, 0), 2) AS unemployment_rate_pct_change
FROM main.district AS d
WHERE d.A12 IS NOT NULL
  AND d.A13 IS NOT NULL
  AND d.district_id IN (
      SELECT a.district_id
      FROM main.loan AS l
      JOIN main.account AS a ON a.account_id = l.account_id
      WHERE l.status = 'D'
  )
ORDER BY d.A2
LIMIT 1000
