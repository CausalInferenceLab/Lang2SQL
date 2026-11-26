WITH cleaned_salaries AS (
    -- Average the recorded amounts as requested. Currency and pay-period labels are stripped.
    SELECT
        Location,
        CompanyName,
        CAST(
            NULLIF(
                REPLACE(
                    LTRIM(SUBSTR(Salary, 1, INSTR(Salary, '/') - 1), '₹$£AFN '),
                    ',', ''
                ),
                ''
            ) AS REAL
        ) AS salary_amount
    FROM main.SalaryDataset
),
national_average AS (
    SELECT AVG(salary_amount) AS country_average
    FROM cleaned_salaries
),
company_averages AS (
    SELECT Location, CompanyName, AVG(salary_amount) AS city_company_average
    FROM cleaned_salaries
    WHERE Location IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
      AND CompanyName IS NOT NULL
      AND TRIM(CompanyName) <> ''
    GROUP BY Location, CompanyName
),
ranked_companies AS (
    SELECT
        Location,
        CompanyName,
        city_company_average,
        ROW_NUMBER() OVER (
            PARTITION BY Location
            ORDER BY city_company_average DESC, CompanyName ASC
        ) AS city_rank
    FROM company_averages
)
SELECT
    r.Location AS "Location",
    r.CompanyName AS "Company Name",
    r.city_company_average AS "Average Salary in State",
    n.country_average AS "Average Salary in Country"
FROM ranked_companies AS r
CROSS JOIN national_average AS n
WHERE r.city_rank <= 5
ORDER BY r.Location, r.city_rank
LIMIT 1000
