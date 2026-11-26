WITH deviations AS (
    SELECT FacRank, FacFirstName, FacLastName, FacSalary,
           ABS(FacSalary - AVG(FacSalary) OVER (PARTITION BY FacRank)) AS deviation
    FROM university_faculty
),
closest AS (
    SELECT deviations.*, MIN(deviation) OVER (PARTITION BY FacRank) AS min_deviation
    FROM deviations
)
SELECT FacRank, FacFirstName, FacLastName, FacSalary
FROM closest
WHERE deviation = min_deviation
ORDER BY FacRank, FacLastName, FacFirstName;
