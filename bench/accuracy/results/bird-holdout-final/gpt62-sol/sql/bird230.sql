WITH ranked_loans AS (
    SELECT l.loan_id, l.account_id, c.client_id, c.birth_date,
           l.amount, l.duration, l.payments, l.status,
           DENSE_RANK() OVER (ORDER BY l.amount DESC) AS amount_rank
    FROM loan AS l
    JOIN disp AS dp ON dp.account_id = l.account_id AND dp.type = 'OWNER'
    JOIN client AS c ON c.client_id = dp.client_id
    WHERE c.gender = 'F'
), selected_loans AS (
    SELECT * FROM ranked_loans WHERE amount_rank <= 3
), account_activity AS (
    SELECT t.account_id,
           COUNT(*) AS transaction_count,
           SUM(CASE WHEN t.type = 'PRIJEM' THEN t.amount ELSE 0 END) AS total_deposits
    FROM trans AS t
    WHERE t.account_id IN (SELECT account_id FROM selected_loans)
    GROUP BY t.account_id
), regional_loan_counts AS (
    SELECT d.A3 AS region, COUNT(*) AS region_loan_count
    FROM loan AS l
    JOIN account AS a ON a.account_id = l.account_id
    JOIN district AS d ON d.district_id = a.district_id
    GROUP BY d.A3
), regional_salaries AS (
    SELECT A3 AS region, ROUND(AVG(A11), 2) AS region_average_salary
    FROM district
    GROUP BY A3
)
SELECT s.client_id,
       ROUND(s.amount, 2) AS loan_amount,
       s.duration AS duration_months,
       ROUND(s.payments * s.duration, 2) AS total_payments,
       ROUND(s.payments * s.duration - s.amount, 2) AS interest_paid,
       s.status AS loan_status,
       d.A2 AS branch_district,
       d.A3 AS branch_region,
       COALESCE(aa.transaction_count, 0) AS transaction_count,
       ROUND(COALESCE(aa.total_deposits, 0), 2) AS total_deposits,
       CASE
           WHEN COALESCE(aa.total_deposits, 0) > 2 * s.amount THEN 'High Income'
           WHEN COALESCE(aa.total_deposits, 0) > s.amount THEN 'Medium Income'
           ELSE 'Low Income'
       END AS income_category,
       rlc.region_loan_count,
       rs.region_average_salary,
       CAST(strftime('%Y', '2026-10-09') AS INTEGER)
           - CAST(strftime('%Y', s.birth_date) AS INTEGER)
           - (strftime('%m-%d', s.birth_date) > strftime('%m-%d', '2026-10-09')) AS completed_age
FROM selected_loans AS s
JOIN account AS a ON a.account_id = s.account_id
JOIN district AS d ON d.district_id = a.district_id
LEFT JOIN account_activity AS aa ON aa.account_id = s.account_id
JOIN regional_loan_counts AS rlc ON rlc.region = d.A3
JOIN regional_salaries AS rs ON rs.region = d.A3
ORDER BY s.amount DESC, s.loan_id, s.client_id;
