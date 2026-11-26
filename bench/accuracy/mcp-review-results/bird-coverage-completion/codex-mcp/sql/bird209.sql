WITH accounts_with_cards AS (
    SELECT d.account_id
    FROM main.card AS c
    JOIN main.disp AS d ON d.disp_id = c.disp_id
)
SELECT ROUND(AVG(district.A11), 2) AS average_district_salary
FROM main.district AS district
WHERE district.A3 = 'central Bohemia'
  AND district.district_id IN (
      SELECT client.district_id
      FROM main.client AS client
      JOIN main.disp AS owner ON owner.client_id = client.client_id
      WHERE owner.type = 'OWNER'
        AND owner.account_id NOT IN (SELECT account_id FROM accounts_with_cards)
  )
LIMIT 1000
