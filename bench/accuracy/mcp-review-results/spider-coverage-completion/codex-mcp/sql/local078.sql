WITH monthly_peaks AS (
    SELECT m.interest_id, m.month_year, m.composition,
           ROW_NUMBER() OVER (
               PARTITION BY m.interest_id
               ORDER BY m.composition DESC, m._year, m._month
           ) AS peak_order
    FROM main.interest_metrics AS m
    WHERE m.month_year IS NOT NULL AND m.composition IS NOT NULL
),
ranked_categories AS (
    SELECT p.month_year, i.interest_name, p.composition,
           ROW_NUMBER() OVER (ORDER BY p.composition DESC, i.id) AS top_rank,
           ROW_NUMBER() OVER (ORDER BY p.composition ASC, i.id) AS bottom_rank
    FROM monthly_peaks AS p
    JOIN main.interest_map AS i ON i.id = p.interest_id
    WHERE p.peak_order = 1
)
SELECT month_year, interest_name, composition
FROM ranked_categories
WHERE top_rank <= 10 OR bottom_rank <= 10
ORDER BY CASE WHEN top_rank <= 10 THEN 0 ELSE 1 END,
         CASE WHEN top_rank <= 10 THEN top_rank ELSE bottom_rank END
LIMIT 20
