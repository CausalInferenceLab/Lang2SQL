WITH salary_distances AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary,
           ABS(FacSalary * COUNT(FacSalary) OVER (PARTITION BY FacRank)
               - SUM(FacSalary) OVER (PARTITION BY FacRank)) AS scaled_distance
    FROM university_faculty
), closest AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary, scaled_distance,
           MIN(scaled_distance) OVER (PARTITION BY FacRank) AS min_scaled_distance
    FROM salary_distances
)
SELECT FacRank, FacFirstName, FacLastName, FacSalary
FROM closest
WHERE scaled_distance = min_scaled_distance
ORDER BY FacRank, FacLastName, FacFirstName, FacSalary;
