WITH account_loans AS (
    SELECT account_id, COUNT(*) AS loan_count, SUM(amount) AS total_amount_borrowed
    FROM loan
    GROUP BY account_id
),
profiles AS (
    SELECT c.client_id, c.gender, c.birth_date, k.type AS card_type,
           CAST(strftime('%Y', k.issued) AS INTEGER)
               - CAST(strftime('%Y', c.birth_date) AS INTEGER) AS age_at_issue,
           COALESCE(l.loan_count, 0) AS loan_count,
           COALESCE(l.total_amount_borrowed, 0) AS total_amount_borrowed,
           d.A2 AS district_name, d.A11 AS district_average_salary
    FROM card AS k
    JOIN disp AS p ON p.disp_id = k.disp_id
    JOIN client AS c ON c.client_id = p.client_id
    JOIN district AS d ON d.district_id = c.district_id
    LEFT JOIN account_loans AS l ON l.account_id = p.account_id
    WHERE k.issued >= '1994-01-01' AND k.issued < '1995-01-01'
)
SELECT client_id, gender, birth_date, card_type, age_at_issue,
       loan_count, total_amount_borrowed,
       CASE
           WHEN loan_count > 0 AND age_at_issue < 30 THEN 'Young borrower'
           WHEN loan_count > 0 AND age_at_issue >= 30 THEN 'Mature borrower'
           ELSE 'Non-borrower'
       END AS borrower_classification,
       district_name, district_average_salary,
       RANK() OVER (PARTITION BY gender ORDER BY age_at_issue DESC) AS age_rank_within_gender
FROM profiles
ORDER BY client_id
LIMIT -1
