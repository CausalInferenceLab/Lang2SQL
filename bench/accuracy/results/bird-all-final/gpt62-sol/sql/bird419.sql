SELECT AVG("HCT") AS average_hematocrit
FROM "Laboratory"
WHERE "Date" >= '1991-01-01'
  AND "Date" < '1992-01-01'
  AND "HCT" IS NOT NULL
  AND CAST(
    CASE
      WHEN TRIM("U-PRO") LIKE '>=%' THEN SUBSTR(TRIM("U-PRO"), 3)
      ELSE TRIM("U-PRO")
    END AS REAL
  ) >= 300;
