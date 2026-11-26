SELECT COUNT(DISTINCT "ID") AS female_admitted_patient_count
FROM "Patient"
WHERE "SEX" = 'F'
  AND "First Date" >= '1998-01-01'
  AND "First Date" < '1999-01-01'
  AND "Admission" LIKE '+%';
