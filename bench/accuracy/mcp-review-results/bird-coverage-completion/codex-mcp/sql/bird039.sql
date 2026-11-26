SELECT County,
       COUNT(*) FILTER (WHERE Virtual = 'F') AS exclusive_virtual_schools
FROM main.schools
WHERE County = 'Santa Barbara'
   OR County = (
       SELECT County
       FROM main.schools
       WHERE CDSCode = '01100170109835'
   )
GROUP BY County
ORDER BY exclusive_virtual_schools DESC, County ASC
LIMIT 2
