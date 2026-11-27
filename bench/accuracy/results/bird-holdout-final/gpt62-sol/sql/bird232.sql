WITH monthly_accounts_by_district AS (
    SELECT district_id, COUNT(*) AS account_count
    FROM account
    WHERE frequency = 'POPLATEK MESICNE'
      AND date >= '1997-01-01'
      AND date < '1998-01-01'
    GROUP BY district_id
)
SELECT COALESCE(SUM(m.account_count), 0) AS total_monthly_statement_accounts,
       COUNT(*) AS qualifying_district_count,
       ROUND(AVG(d.A11), 2) AS average_salary
FROM monthly_accounts_by_district AS m
JOIN district AS d ON d.district_id = m.district_id
WHERE CAST(d.A4 AS INTEGER) >= 100000;
