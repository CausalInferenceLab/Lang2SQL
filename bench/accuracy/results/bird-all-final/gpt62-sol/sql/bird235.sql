WITH classic_owners AS (
    SELECT DISTINCT a.district_id, a.account_id, d.client_id
    FROM disp AS d
    JOIN account AS a ON a.account_id = d.account_id
    WHERE d.type = 'OWNER'
      AND EXISTS (
          SELECT 1
          FROM card AS c
          WHERE c.disp_id = d.disp_id AND c.type = 'classic'
      )
), owner_counts AS (
    SELECT district_id, COUNT(DISTINCT client_id) AS client_count
    FROM classic_owners
    GROUP BY district_id
), classic_accounts AS (
    SELECT DISTINCT district_id, account_id
    FROM classic_owners
), payments_by_destination AS (
    SELECT a.district_id, t.bank, t."account", SUM(t.amount) AS amount_paid
    FROM classic_accounts AS a
    JOIN trans AS t ON t.account_id = a.account_id
    WHERE t.type = 'VYDAJ'
      AND t.bank IS NOT NULL
      AND t."account" IS NOT NULL
    GROUP BY a.district_id, t.bank, t."account"
), payment_totals AS (
    SELECT district_id, COUNT(*) AS external_account_count,
           SUM(amount_paid) AS total_amount_paid
    FROM payments_by_destination
    GROUP BY district_id
)
SELECT d.A2 AS district_name,
       COALESCE(o.client_count, 0) AS client_count,
       COALESCE(p.external_account_count, 0) AS external_account_count,
       COALESCE(p.total_amount_paid, 0) AS total_amount_paid
FROM district AS d
LEFT JOIN owner_counts AS o ON o.district_id = d.district_id
LEFT JOIN payment_totals AS p ON p.district_id = d.district_id
ORDER BY total_amount_paid DESC, district_name ASC;
