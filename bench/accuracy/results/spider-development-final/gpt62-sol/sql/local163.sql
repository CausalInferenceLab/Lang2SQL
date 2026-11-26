WITH salary_distances AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary,
           ABS(FacSalary - AVG(FacSalary) OVER (PARTITION BY FacRank)) AS salary_distance
    FROM university_faculty
), rank_minimums AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary, salary_distance,
           MIN(salary_distance) OVER (PARTITION BY FacRank) AS min_distance
    FROM salary_distances
)
SELECT FacRank, FacFirstName, FacLastName, FacSalary
FROM rank_minimums
WHERE salary_distance = min_distance
ORDER BY FacRank, FacLastName, FacFirstName;
