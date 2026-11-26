SELECT
    1.0 * COUNT(*) FILTER (WHERE DOCType = 'Unified School District')
    / NULLIF(COUNT(*) FILTER (WHERE DOCType = 'Elementary School District'), 0) AS ratio
FROM main.schools
WHERE County = 'Orange'
  AND StatusType = 'Merged'
LIMIT 1
