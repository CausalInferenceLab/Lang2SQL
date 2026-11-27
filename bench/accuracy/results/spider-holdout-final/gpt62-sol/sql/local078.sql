WITH monthly_peaks AS (
    SELECT
        im.id AS interest_id,
        im.interest_name,
        m._month,
        m._year,
        m.composition,
        ROW_NUMBER() OVER (
            PARTITION BY im.id
            ORDER BY m.composition DESC, m._year ASC NULLS LAST, m._month ASC NULLS LAST
        ) AS month_rank
    FROM interest_metrics AS m
    JOIN interest_map AS im ON im.id = m.interest_id
    WHERE m.composition IS NOT NULL
), ranked_interests AS (
    SELECT
        CASE WHEN _month IS NOT NULL AND _year IS NOT NULL
             THEN printf('%02d-%04d', _month, _year)
        END AS time,
        interest_name,
        composition,
        ROW_NUMBER() OVER (ORDER BY composition DESC, interest_id) AS top_rank,
        ROW_NUMBER() OVER (ORDER BY composition ASC, interest_id) AS bottom_rank
    FROM monthly_peaks
    WHERE month_rank = 1
), selected_interests AS (
    SELECT time, interest_name, composition, 1 AS sort_group, top_rank AS sort_rank
    FROM ranked_interests
    WHERE top_rank <= 10
    UNION ALL
    SELECT time, interest_name, composition, 2 AS sort_group, bottom_rank AS sort_rank
    FROM ranked_interests
    WHERE bottom_rank <= 10
)
SELECT time, interest_name, composition
FROM selected_interests
ORDER BY sort_group, sort_rank;
