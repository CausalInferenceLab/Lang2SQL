WITH top_loans AS (
    SELECT a.district_id
    FROM loan AS l
    JOIN account AS a ON a.account_id = l.account_id
    WHERE l.duration = 24
      AND a.frequency = 'POPLATEK TYDNE'
    ORDER BY l.amount DESC, l.loan_id ASC
    LIMIT 10
), top_districts AS (
    SELECT DISTINCT district_id
    FROM top_loans
)
SELECT d.A2 AS district_name,
       CAST(d.A4 AS INTEGER) AS inhabitants,
       ROUND(AVG(d.A11) OVER (), 2) AS avg_yearly_salary
FROM top_districts AS td
JOIN district AS d ON d.district_id = td.district_id
ORDER BY inhabitants DESC, d.district_id ASC
LIMIT 1;
