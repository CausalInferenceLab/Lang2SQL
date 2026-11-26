WITH top_districts AS (
    SELECT district_id, A2 AS district_name
    FROM district
    ORDER BY CAST(A4 AS INTEGER) DESC, district_id
    LIMIT 5
), active_loans_by_account AS (
    SELECT account_id, SUM(amount) AS active_loan_amount
    FROM loan
    WHERE status IN ('C', 'D')
    GROUP BY account_id
), district_averages AS (
    SELECT d.district_id, d.district_name,
           AVG(CASE WHEN a.account_id IS NOT NULL
                    THEN COALESCE(l.active_loan_amount, 0) END) AS avg_active_loan
    FROM top_districts AS d
    LEFT JOIN account AS a ON a.district_id = d.district_id
    LEFT JOIN active_loans_by_account AS l ON l.account_id = a.account_id
    GROUP BY d.district_id, d.district_name
)
SELECT district_name,
       CAST(ROUND(avg_active_loan, 0) AS INTEGER) AS avg_active_loan_per_account
FROM district_averages
ORDER BY avg_active_loan DESC, district_name, district_id;
