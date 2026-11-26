SELECT "Diagnosis"
FROM "Patient"
WHERE "First Date" >= '1998-02-15'
  AND "First Date" < '1998-03-01'
  AND "Admission" LIKE '+%'
ORDER BY julianday("First Date") - julianday("Birthday") DESC, "ID" ASC
LIMIT 1;
