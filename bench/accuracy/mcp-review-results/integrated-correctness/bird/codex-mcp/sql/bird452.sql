SELECT e."Symptoms", p."Diagnosis" AS "Overall Diagnosis"
FROM main."Examination" AS e
JOIN main."Patient" AS p ON p."ID" = e."ID"
WHERE INSTR(',' || REPLACE(REPLACE(e."Diagnosis", ' ', ''), '+', ',') || ',', ',SLE,') > 0
  AND NULLIF(TRIM(e."Symptoms"), '') IS NOT NULL
  AND p."Birthday" IS NOT NULL
ORDER BY p."Birthday" DESC, p."ID" ASC, e."Examination Date" DESC
LIMIT 1
