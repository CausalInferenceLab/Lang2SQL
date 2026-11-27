WITH cardholders AS (
    SELECT
        cl.client_id,
        cl.gender,
        cl.birth_date,
        ca.type AS card_type,
        CAST(strftime('%Y', ca.issued) AS INTEGER)
            - CAST(strftime('%Y', cl.birth_date) AS INTEGER) AS age_at_issue,
        dp.account_id,
        cl.district_id
    FROM card AS ca
    JOIN disp AS dp ON dp.disp_id = ca.disp_id
    JOIN client AS cl ON cl.client_id = dp.client_id
    WHERE ca.issued >= '1994-01-01' AND ca.issued < '1995-01-01'
), account_loans AS (
    SELECT account_id, COUNT(*) AS loan_count, SUM(amount) AS total_amount_borrowed
    FROM loan
    GROUP BY account_id
)
SELECT
    ch.client_id,
    ch.gender,
    ch.birth_date,
    ch.card_type,
    ch.age_at_issue,
    COALESCE(al.loan_count, 0) AS loan_count,
    COALESCE(al.total_amount_borrowed, 0) AS total_amount_borrowed,
    CASE
        WHEN al.loan_count > 0 AND ch.age_at_issue < 30 THEN 'Young borrower'
        WHEN al.loan_count > 0 AND ch.age_at_issue >= 30 THEN 'Mature borrower'
        ELSE 'Non-borrower'
    END AS borrower_classification,
    d.A2 AS district_name,
    d.A11 AS district_average_salary,
    RANK() OVER (PARTITION BY ch.gender ORDER BY ch.age_at_issue DESC) AS age_rank_within_gender
FROM cardholders AS ch
LEFT JOIN account_loans AS al ON al.account_id = ch.account_id
LEFT JOIN district AS d ON d.district_id = ch.district_id
ORDER BY ch.client_id;
