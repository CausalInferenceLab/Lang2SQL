WITH target_counties (county) AS (
    SELECT 'Santa Barbara'
    UNION
    SELECT County
    FROM schools
    WHERE CDSCode = '01100170109835'
)
SELECT t.county AS county,
       COUNT(DISTINCT s.CDSCode) AS exclusive_virtual_schools
FROM target_counties AS t
LEFT JOIN schools AS s
    ON s.County = t.county AND s.Virtual = 'F'
GROUP BY t.county
ORDER BY exclusive_virtual_schools DESC, county ASC;
