SELECT AVG(1999 - CAST(strftime('%Y', p.Birthday) AS INTEGER)) AS average_age_years
FROM Patient AS p
WHERE EXISTS (
    SELECT 1
    FROM Laboratory AS l
    WHERE l.ID = p.ID
      AND l."U-PRO" IN ('3', '4', '300', '>=300', '1000', '>=1000')
);
