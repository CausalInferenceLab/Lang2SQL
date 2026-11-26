SELECT COUNT(DISTINCT c.name) AS creature_count
FROM cards AS c
WHERE (',' || c.types || ',') LIKE '%,Creature,%'
  AND CAST(c.power AS INTEGER) >= 10
  AND EXISTS (
    SELECT 1
    FROM set_translations AS st
    WHERE st.setCode = c.setCode
      AND st.language = 'French'
      AND NULLIF(TRIM(st.translation), '') IS NOT NULL
  );
