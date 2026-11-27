WITH loan_totals AS (
    SELECT account_id, SUM(amount) AS total_amount_borrowed
    FROM loan
    GROUP BY account_id
),
prachatice_borrowers AS (
    SELECT a.account_id, a.date AS account_opening_date, l.total_amount_borrowed
    FROM account AS a
    JOIN district AS branch ON branch.district_id = a.district_id
    JOIN loan_totals AS l ON l.account_id = a.account_id
    WHERE branch.A2 = 'Prachatice'
),
counterparty_counts AS (
    SELECT t.account_id, COUNT(DISTINCT t."account") AS outside_counterparty_count
    FROM trans AS t
    JOIN prachatice_borrowers AS b ON b.account_id = t.account_id
    WHERE t.operation IN ('PREVOD NA UCET', 'PREVOD Z UCTU')
    GROUP BY t.account_id
)
SELECT
    b.account_id AS account_number,
    b.account_opening_date,
    b.total_amount_borrowed,
    COALESCE(cp.outside_counterparty_count, 0) AS outside_counterparty_count,
    home.A2 AS home_district,
    CAST(home.A4 AS INTEGER) AS home_district_population,
    RANK() OVER (ORDER BY CAST(home.A4 AS INTEGER) DESC) AS population_rank
FROM prachatice_borrowers AS b
JOIN disp AS d ON d.account_id = b.account_id AND d.type = 'OWNER'
JOIN client AS c ON c.client_id = d.client_id
JOIN district AS home ON home.district_id = c.district_id
LEFT JOIN counterparty_counts AS cp ON cp.account_id = b.account_id
ORDER BY home_district_population DESC, account_number ASC;
