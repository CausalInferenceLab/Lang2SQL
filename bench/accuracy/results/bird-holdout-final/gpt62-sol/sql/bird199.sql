SELECT d.A2 AS district,
       ROUND(100.0 * (d.A13 - d.A12) / d.A12, 2) AS unemployment_pct_change
FROM district AS d
WHERE d.A12 IS NOT NULL
  AND d.A13 IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM account AS a
      JOIN loan AS l ON l.account_id = a.account_id
      WHERE a.district_id = d.district_id
        AND l.status = 'D'
  );
