WITH CleanedSalary AS (
  SELECT
    Location,
    CompanyName,
    CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(Salary, '$', ''), ',', ''), ' ', ''), 'Rs.', ''), '₹', '') AS INTEGER) AS CleanSalary
  FROM SalaryDataset
  WHERE Salary IS NOT NULL
),
AvgCompanySalaryByCity AS (
  SELECT
    Location,
    CompanyName,
    AVG(CleanSalary) AS AvgSalaryCity
  FROM CleanedSalary
  WHERE Location IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
  GROUP BY Location, CompanyName
),
Top5CompaniesByCity AS (
  SELECT
    Location,
    CompanyName,
    AvgSalaryCity,
    ROW_NUMBER() OVER (PARTITION BY Location ORDER BY AvgSalaryCity DESC) AS Rank
  FROM AvgCompanySalaryByCity
),
AvgNationalSalary AS (
  SELECT
    AVG(CleanSalary) AS AvgSalaryCountry
  FROM CleanedSalary
)
SELECT
  t.Location AS Location,
  t.CompanyName AS "Company Name",
  t.AvgSalaryCity AS "Average Salary in State",
  n.AvgSalaryCountry AS "Average Salary in Country"
FROM Top5CompaniesByCity t
CROSS JOIN AvgNationalSalary n
WHERE t.Rank <= 5
ORDER BY t.Location, t.AvgSalaryCity DESC
;
