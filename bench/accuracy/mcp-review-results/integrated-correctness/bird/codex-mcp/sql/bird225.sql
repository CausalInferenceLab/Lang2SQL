WITH cardholders AS (
    SELECT
        cl.client_id,
        cl.gender,
        cl.birth_date,
        c.type AS card_type,
        CAST(strftime('%Y', c.issued) AS INTEGER)
            - CAST(strftime('%Y', cl.birth_date) AS INTEGER) AS age_at_card_issue,
        dp.account_id,
        cl.district_id
    FROM main.card AS c
    JOIN main.disp AS dp ON dp.disp_id = c.disp_id
    JOIN main.client AS cl ON cl.client_id = dp.client_id
    WHERE c.issued >= '1994-01-01' AND c.issued < '1995-01-01'
),
account_loans AS (
    SELECT
        l.account_id,
        COUNT(*) AS number_of_loans,
        SUM(l.amount) AS total_amount_borrowed
    FROM main.loan AS l
    GROUP BY l.account_id
)
SELECT
    ch.client_id,
    ch.gender,
    ch.birth_date,
    ch.card_type,
    ch.age_at_card_issue,
    COALESCE(al.number_of_loans, 0) AS number_of_loans,
    COALESCE(al.total_amount_borrowed, 0) AS total_amount_borrowed,
    CASE
        WHEN al.number_of_loans > 0 AND ch.age_at_card_issue < 30 THEN 'Young borrower'
        WHEN al.number_of_loans > 0 AND ch.age_at_card_issue >= 30 THEN 'Mature borrower'
        ELSE 'Non-borrower'
    END AS borrower_classification,
    dt.A2 AS district_name,
    dt.A11 AS district_average_salary,
    RANK() OVER (
        PARTITION BY ch.gender
        ORDER BY ch.age_at_card_issue DESC
    ) AS age_rank_within_gender
FROM cardholders AS ch
LEFT JOIN account_loans AS al ON al.account_id = ch.account_id
LEFT JOIN main.district AS dt ON dt.district_id = ch.district_id
ORDER BY ch.client_id
LIMIT 1000
