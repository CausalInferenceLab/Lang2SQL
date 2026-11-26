SELECT COUNT(DISTINCT p."ID") AS patient_count
FROM "Patient" AS p
JOIN "Examination" AS e ON e."ID" = p."ID"
WHERE e."Examination Date" >= '1997-01-01'
  AND e."Examination Date" < '1998-01-01'
  AND e."Thrombosis" > 0;
