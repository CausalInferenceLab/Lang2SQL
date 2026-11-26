SELECT 1.0 * COUNT(CASE WHEN DOCType = 'Unified School District' THEN 1 END)
       / NULLIF(COUNT(CASE WHEN DOCType = 'Elementary School District' THEN 1 END), 0) AS ratio
FROM schools
WHERE County = 'Orange'
  AND StatusType = 'Merged';
