WITH CleanedSalary AS (
  SELECT 
    CompanyName,
    Location,
    CAST(REPLACE(REPLACE(REPLACE(Salary, '₹', ''), ',', ''), '/yr', '') AS INTEGER) AS SalaryNumeric
  FROM SalaryDataset
  WHERE Location IN ('Mumbai', 'Pune', 'New Delhi', 'Hyderabad')
    AND Salary IS NOT NULL
    AND TRIM(Salary) <> ''
),
LocationCompanyAvg AS (
  SELECT 
    Location,
    CompanyName,
    AVG(SalaryNumeric) AS AvgSalaryInState
  FROM CleanedSalary
  GROUP BY Location, CompanyName
),
NationalSalary AS (
  SELECT 
    CompanyName,
    AVG(CAST(REPLACE(REPLACE(REPLACE(Salary, '₹', ''), ',', ''), '/yr', '') AS INTEGER)) AS AvgSalaryInCountry
  FROM SalaryDataset
  WHERE Salary IS NOT NULL AND TRIM(Salary) <> ''
  GROUP BY CompanyName
),
RankedCompany AS (
  SELECT 
    lca.Location,
    lca.CompanyName,
    lca.AvgSalaryInState,
    ns.AvgSalaryInCountry,
    ROW_NUMBER() OVER (PARTITION BY lca.Location ORDER BY lca.AvgSalaryInState DESC) AS rn
  FROM LocationCompanyAvg lca
  JOIN NationalSalary ns ON lca.CompanyName = ns.CompanyName
)
SELECT 
  Location,
  CompanyName,
  AvgSalaryInState,
  AvgSalaryInCountry
FROM RankedCompany
WHERE rn <= 5
ORDER BY Location, AvgSalaryInState DESC;
