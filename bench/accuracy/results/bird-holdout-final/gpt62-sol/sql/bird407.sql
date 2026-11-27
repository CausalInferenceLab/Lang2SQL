SELECT MIN(CAST(strftime('%Y', l."Date") AS INTEGER) - CAST(strftime('%Y', p.Birthday) AS INTEGER)) AS youngest_age
FROM Laboratory AS l
JOIN Patient AS p ON p.ID = l.ID
WHERE CAST(l.RNP AS INTEGER) >= 64;
