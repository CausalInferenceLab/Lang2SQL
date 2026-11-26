WITH branch_accounts AS (
    SELECT a.account_id, a.date AS opened_date
    FROM account AS a
    JOIN district AS d ON d.district_id = a.district_id
    WHERE d.A2 = 'Tabor'
      AND a.date < '1997-01-01'
), card_counts AS (
    SELECT dp.account_id, COUNT(*) AS credit_card_count
    FROM branch_accounts AS a
    JOIN disp AS dp ON dp.account_id = a.account_id
    JOIN card AS ca ON ca.disp_id = dp.disp_id
    WHERE ca.issued < '1997-01-01'
    GROUP BY dp.account_id
), loan_summary AS (
    SELECT l.account_id,
           COUNT(*) AS loan_count,
           MAX(CASE WHEN l.status = 'B' THEN 1 ELSE 0 END) AS has_default,
           MAX(CASE WHEN l.status IN ('C', 'D') THEN 1 ELSE 0 END) AS has_repaying
    FROM branch_accounts AS a
    JOIN loan AS l ON l.account_id = a.account_id
    WHERE l.date < '1997-01-01'
    GROUP BY l.account_id
), activity AS (
    SELECT t.account_id,
           COUNT(*) AS transaction_count_1997,
           SUM(CASE WHEN t.type = 'PRIJEM' THEN t.amount ELSE 0 END) AS money_in_1997,
           SUM(CASE WHEN t.type IN ('VYDAJ', 'VYBER') THEN t.amount ELSE 0 END) AS money_out_1997,
           MAX(t.balance) AS peak_balance_1997
    FROM branch_accounts AS a
    JOIN trans AS t ON t.account_id = a.account_id
    WHERE t.date >= '1997-01-01' AND t.date < '1998-01-01'
    GROUP BY t.account_id
)
SELECT a.account_id,
       a.opened_date,
       c.gender AS owner_gender,
       1996 - CAST(strftime('%Y', c.birth_date) AS INTEGER) AS owner_age,
       COALESCE(cc.credit_card_count, 0) AS credit_card_count,
       COALESCE(ls.loan_count, 0) AS loan_count,
       CASE
           WHEN ls.loan_count IS NULL THEN 'no loan'
           WHEN ls.has_default = 1 THEN 'defaulted'
           WHEN ls.has_repaying = 1 THEN 'repaying'
           ELSE 'paid off'
       END AS loan_standing,
       COALESCE(t.transaction_count_1997, 0) AS transaction_count_1997,
       COALESCE(t.money_in_1997, 0) AS money_in_1997,
       COALESCE(t.money_out_1997, 0) AS money_out_1997,
       COALESCE(t.peak_balance_1997, 0) AS peak_balance_1997,
       RANK() OVER (ORDER BY COALESCE(t.peak_balance_1997, 0) DESC) AS peak_balance_rank
FROM branch_accounts AS a
JOIN disp AS owner ON owner.account_id = a.account_id AND owner.type = 'OWNER'
JOIN client AS c ON c.client_id = owner.client_id
LEFT JOIN card_counts AS cc ON cc.account_id = a.account_id
LEFT JOIN loan_summary AS ls ON ls.account_id = a.account_id
LEFT JOIN activity AS t ON t.account_id = a.account_id
ORDER BY peak_balance_rank, a.account_id;
