WITH loans_per_account AS (
    SELECT a.account_id, COUNT(l.loan_id) AS loan_count
    FROM account AS a
    LEFT JOIN loan AS l ON l.account_id = a.account_id
    WHERE EXISTS (
        SELECT 1
        FROM disp AS d
        JOIN client AS c ON c.client_id = d.client_id
        WHERE d.account_id = a.account_id
          AND d.type = 'OWNER'
          AND c.birth_date < '1950-01-01'
    )
    GROUP BY a.account_id
)
SELECT ROUND(AVG(loan_count), 2) AS average_loans_per_account
FROM loans_per_account;
