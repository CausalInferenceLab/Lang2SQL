SELECT
    s.City,
    COUNT(*) AS magnet_school_count
FROM main.schools AS s
JOIN main.frpm AS f ON f.CDSCode = s.CDSCode
WHERE s.Magnet = 1
  AND s.City IS NOT NULL
  AND f."NSLP Provision Status" IN (
      'Breakfast Provision 2',
      'Lunch Provision 2',
      'Provision 2'
  )
GROUP BY s.City
ORDER BY magnet_school_count DESC, s.City
LIMIT 1000
