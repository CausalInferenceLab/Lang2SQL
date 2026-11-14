WITH unicorns AS (
  SELECT cd.company_id, cd.year_founded
  FROM companies_funding cf
  JOIN companies_dates cd ON cf.company_id = cd.company_id
  WHERE cf.valuation >= 1000000000 AND cd.year_founded BETWEEN 2019 AND 2021
),
industry_unicorns AS (
  SELECT ci.industry, u.year_founded
  FROM companies_industries ci
  JOIN unicorns u ON ci.company_id = u.company_id
),
industry_counts AS (
  SELECT industry, COUNT(*) AS total_unicorns
  FROM industry_unicorns
  GROUP BY industry
),
top_industry AS (
  SELECT industry
  FROM industry_counts
  ORDER BY total_unicorns DESC
  LIMIT 1
),
yearly_counts AS (
  SELECT iu.year_founded, COUNT(*) AS count
  FROM industry_unicorns iu
  JOIN top_industry ti ON iu.industry = ti.industry
  GROUP BY iu.year_founded
)
SELECT AVG(count) AS avg_new_unicorns_per_year
FROM yearly_counts;
