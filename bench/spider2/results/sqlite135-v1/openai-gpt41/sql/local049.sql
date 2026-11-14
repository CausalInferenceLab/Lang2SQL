WITH unicorns_2019_2021 AS (
  SELECT cd.company_id, cd.year
  FROM companies_dates cd
  JOIN companies_funding cf ON cd.company_id = cf.company_id
  WHERE cd.year BETWEEN 2019 AND 2021
    AND cf.valuation >= 1000000000
),
industry_counts AS (
  SELECT ci.industry, COUNT(DISTINCT u.company_id) AS unicorn_count
  FROM unicorns_2019_2021 u
  JOIN companies_industries ci ON u.company_id = ci.company_id
  GROUP BY ci.industry
),
top_industry AS (
  SELECT industry
  FROM industry_counts
  ORDER BY unicorn_count DESC
  LIMIT 1
),
yearly_new_unicorns AS (
  SELECT u.year, COUNT(DISTINCT u.company_id) AS num_new_unicorns
  FROM unicorns_2019_2021 u
  JOIN companies_industries ci ON u.company_id = ci.company_id
  WHERE ci.industry = (SELECT industry FROM top_industry)
  GROUP BY u.year
)
SELECT AVG(num_new_unicorns) AS avg_new_unicorns_per_year
FROM yearly_new_unicorns;
