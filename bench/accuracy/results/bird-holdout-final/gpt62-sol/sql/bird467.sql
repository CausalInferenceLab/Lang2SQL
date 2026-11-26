SELECT COUNT(DISTINCT p.ID) AS patient_count
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID
WHERE date(l."Date") >= '1981-01-01'
  AND date(l."Date") < '1987-01-01'
  AND (
    CAST(strftime('%Y', l."Date") AS INTEGER)
    - CAST(strftime('%Y', p.Birthday) AS INTEGER)
    - (strftime('%m-%d', l."Date") < strftime('%m-%d', p.Birthday))
  ) < 50
  AND l.PLT BETWEEN 100 AND 400
  AND (
    UPPER(TRIM(l."U-PRO")) = 'TR'
    OR TRIM(l."U-PRO") GLOB '%*%'
    OR CAST(TRIM(l."U-PRO") AS REAL) > 0
  );
