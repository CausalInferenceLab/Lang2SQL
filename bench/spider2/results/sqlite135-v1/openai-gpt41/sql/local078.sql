WITH comp_per_cat AS (
  SELECT 
    imap.interest_name,
    imetrics.month_year,
    imetrics.composition,
    ROW_NUMBER() OVER (PARTITION BY imap.interest_name ORDER BY imetrics.composition DESC) AS rn
  FROM interest_metrics imetrics
  JOIN interest_map imap ON imetrics.interest_id = imap.id
),
max_comp_per_cat AS (
  SELECT 
    interest_name,
    month_year,
    composition
  FROM comp_per_cat
  WHERE rn = 1
),
ranked_top AS (
  SELECT 
    'top' as category_rank_type,
    interest_name, 
    month_year,
    composition
  FROM max_comp_per_cat
  ORDER BY composition DESC
  LIMIT 10
),
ranked_bottom AS (
  SELECT 
    'bottom' as category_rank_type,
    interest_name, 
    month_year,
    composition
  FROM max_comp_per_cat
  ORDER BY composition ASC
  LIMIT 10
)
SELECT * FROM ranked_top
UNION ALL
SELECT * FROM ranked_bottom;
