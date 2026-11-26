SELECT s.City
FROM schools AS s
WHERE s.CDSCode = '01611920113902'
  AND s.County = 'Alameda'
  AND s.EILCode = 'HS'
  AND EXISTS (
    SELECT 1
    FROM frpm AS f
    WHERE f.CDSCode = s.CDSCode
      AND f."NSLP Provision Status" = 'Breakfast Provision 2'
      AND f."Low Grade" = '9'
      AND f."High Grade" = '12'
  );
