SELECT COUNT(*) AS total_printings
FROM (
    SELECT DISTINCT setCode, number, name
    FROM cards
    WHERE CAST(power AS REAL) > 3
      AND CAST(power AS REAL) <= 10
) AS matching_printings;
