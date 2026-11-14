WITH female_legislators AS (
  SELECT id
  FROM legislators
  WHERE gender = 'F'
),
legislators_terms_with_dec31 AS (
  SELECT lt.legislator_id, lt.state
  FROM legislators_terms lt
  JOIN female_legislators fl ON lt.legislator_id = fl.id
  WHERE (
    -- Check if any part of the term includes December 31st in any year
    strftime('%m-%d', lt.start_date) <= '12-31'
    AND strftime('%m-%d', IFNULL(lt.end_date, '9999-12-31')) >= '12-31'
  )
),
first_state_per_legislator AS (
  SELECT lt.legislator_id, lt.state,
    ROW_NUMBER() OVER (PARTITION BY lt.legislator_id ORDER BY lt.start_date) as rn
  FROM legislators_terms lt
  JOIN female_legislators fl ON lt.legislator_id = fl.id
)
SELECT ltw.state, COUNT(DISTINCT ltw.legislator_id) AS count
FROM legislators_terms_with_dec31 ltw
JOIN (
  SELECT legislator_id, state
  FROM first_state_per_legislator
  WHERE rn = 1
) fs ON ltw.legislator_id = fs.legislator_id AND ltw.state = fs.state
GROUP BY ltw.state
ORDER BY count DESC
LIMIT 1;
