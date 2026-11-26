WITH latest_dates AS (
    SELECT "ID",
           MAX("Date") AS latest_sample_date,
           MAX(CASE WHEN "SC170" IS NOT NULL THEN "Date" END) AS latest_scl70_date
    FROM "Laboratory"
    GROUP BY "ID"
)
SELECT d."ID"
FROM latest_dates AS d
JOIN "Laboratory" AS l
  ON l."ID" = d."ID"
 AND l."Date" = d.latest_scl70_date
WHERE d.latest_sample_date > '1997-01-01'
  AND l."SC170" IS NOT NULL
GROUP BY d."ID"
HAVING MIN(CASE WHEN l."SC170" IN ('negative', '0') THEN 1 ELSE 0 END) = 1
ORDER BY d."ID";
