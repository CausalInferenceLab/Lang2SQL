WITH rank_averages AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary,
           AVG(FacSalary) OVER (PARTITION BY FacRank) AS avg_salary
    FROM university_faculty
    WHERE FacRank IS NOT NULL AND FacSalary IS NOT NULL
), closest AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary,
           DENSE_RANK() OVER (
               PARTITION BY FacRank ORDER BY ABS(FacSalary - avg_salary)
           ) AS proximity_rank
    FROM rank_averages
)
SELECT FacRank, FacFirstName, FacLastName, FacSalary
FROM closest
WHERE proximity_rank = 1
ORDER BY FacRank, FacLastName, FacFirstName, FacSalary;
