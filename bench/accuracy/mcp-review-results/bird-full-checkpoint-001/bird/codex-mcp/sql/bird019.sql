SELECT f."Low Grade" AS lowest_grade
FROM main.schools AS s
JOIN main.frpm AS f ON f.CDSCode = s.CDSCode
WHERE s.NCESDist = '0632640'
  AND f."Low Grade" IS NOT NULL
ORDER BY CASE f."Low Grade"
    WHEN 'P' THEN -1
    WHEN 'K' THEN 0
    ELSE CAST(f."Low Grade" AS INTEGER)
END
LIMIT 1
