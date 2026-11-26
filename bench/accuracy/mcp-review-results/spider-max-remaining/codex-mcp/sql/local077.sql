WITH interest_scores AS (
    SELECT
        printf('%04d-%02d-01', m._year, m._month) AS month_start,
        m.interest_id,
        m.composition / NULLIF(m.index_value, 0.0) AS avg_composition
    FROM main.interest_metrics AS m
),
ranked_interests AS (
    SELECT
        month_start,
        interest_id,
        avg_composition,
        ROW_NUMBER() OVER (
            PARTITION BY month_start
            ORDER BY avg_composition DESC, interest_id
        ) AS monthly_rank
    FROM interest_scores
    WHERE month_start BETWEEN '2018-07-01' AND '2019-08-01'
      AND avg_composition IS NOT NULL
),
monthly_history AS (
    SELECT
        r.month_start AS date,
        i.interest_name,
        r.avg_composition AS max_index_composition,
        AVG(r.avg_composition) OVER (
            by_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3_month_avg,
        LAG(i.interest_name, 1) OVER by_month AS interest_name_1_month_ago,
        LAG(r.avg_composition, 1) OVER by_month AS max_index_composition_1_month_ago,
        LAG(i.interest_name, 2) OVER by_month AS interest_name_2_months_ago,
        LAG(r.avg_composition, 2) OVER by_month AS max_index_composition_2_months_ago
    FROM ranked_interests AS r
    JOIN main.interest_map AS i ON i.id = r.interest_id
    WHERE r.monthly_rank = 1
    WINDOW by_month AS (ORDER BY r.month_start)
)
SELECT *
FROM monthly_history
WHERE date >= '2018-09-01'
ORDER BY date
LIMIT 12
