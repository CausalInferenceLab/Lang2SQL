WITH yearly_totals AS (
    SELECT
        SUM(CASE WHEN l.date < '1997-01-01' THEN l.amount END) AS total_1996,
        SUM(CASE WHEN l.date >= '1997-01-01' THEN l.amount END) AS total_1997
    FROM loan AS l
    WHERE l.date >= '1996-01-01'
      AND l.date < '1998-01-01'
      AND EXISTS (
          SELECT 1
          FROM disp AS d
          JOIN client AS c ON c.client_id = d.client_id
          WHERE d.account_id = l.account_id
            AND d.type = 'OWNER'
            AND c.gender = 'M'
      )
)
SELECT 100.0 * (total_1997 - total_1996) / NULLIF(total_1996, 0) AS percentage_growth
FROM yearly_totals;
