SELECT e."Symptoms", p."Diagnosis" AS "Overall Diagnosis"
FROM "Examination" AS e
JOIN "Patient" AS p ON p."ID" = e."ID"
WHERE NULLIF(TRIM(e."Symptoms"), '') IS NOT NULL
  AND INSTR(
        ',' || REPLACE(REPLACE(REPLACE(UPPER(e."Diagnosis"), ' ', ''), '+', ','), '->', ',') || ',',
        ',SLE,'
      ) > 0
ORDER BY p."Birthday" DESC, p."ID" ASC, e."Examination Date" DESC
LIMIT 1;
