SELECT
    1.0 * COUNT(DISTINCT CASE WHEN p.SEX = 'M' AND l.UA > 8.0 THEN p.ID END)
    / NULLIF(COUNT(DISTINCT CASE WHEN p.SEX = 'F' AND l.UA > 6.5 THEN p.ID END), 0)
    AS male_to_female_ratio
FROM Patient AS p
JOIN Laboratory AS l ON l.ID = p.ID;
