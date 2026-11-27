WITH RECURSIVE salary_characters (Location, CompanyName, remaining, digits) AS (
    SELECT Location, CompanyName, COALESCE(Salary, ''), ''
    FROM SalaryDataset

    UNION ALL

    SELECT Location,
           CompanyName,
           SUBSTR(remaining, 2),
           digits || CASE
               WHEN SUBSTR(remaining, 1, 1) GLOB '[0-9]'
               THEN SUBSTR(remaining, 1, 1)
               ELSE ''
           END
    FROM salary_characters
    WHERE remaining <> ''
),
cleaned_salaries AS (
    SELECT Location,
           CompanyName,
           CAST(NULLIF(digits, '') AS REAL) AS salary_amount
    FROM salary_characters
    WHERE remaining = ''
),
national_average AS (
    SELECT AVG(salary_amount) AS country_average
    FROM cleaned_salaries
),
city_company_averages AS (
    SELECT Location, CompanyName, AVG(salary_amount) AS city_company_average
    FROM cleaned_salaries
    WHERE Location IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
    GROUP BY Location, CompanyName
    HAVING COUNT(salary_amount) > 0
),
ranked_companies AS (
    SELECT Location,
           CompanyName,
           city_company_average,
           ROW_NUMBER() OVER (
               PARTITION BY Location
               ORDER BY city_company_average DESC, CompanyName ASC
           ) AS city_rank
    FROM city_company_averages
)
SELECT r.Location AS "Location",
       r.CompanyName AS "Company Name",
       r.city_company_average AS "Average Salary in State",
       n.country_average AS "Average Salary in Country"
FROM ranked_companies AS r
CROSS JOIN national_average AS n
WHERE r.city_rank <= 5
ORDER BY r.Location, r.city_rank;
