WITH interest_monthly AS (
    SELECT printf('%04d-%02d-01', _year, _month) AS month_date,
           interest_id,
           AVG(1.0 * composition / NULLIF(index_value, 0)) AS index_composition
    FROM interest_metrics
    WHERE _year * 100 + _month BETWEEN 201807 AND 201908
    GROUP BY 1, 2
), ranked AS (
    SELECT month_date,
           interest_id,
           index_composition,
           ROW_NUMBER() OVER (
               PARTITION BY month_date
               ORDER BY index_composition DESC, interest_id
           ) AS monthly_rank
    FROM interest_monthly
), monthly_top AS (
    SELECT r.month_date,
           m.interest_name,
           r.index_composition AS max_index_composition
    FROM ranked AS r
    JOIN interest_map AS m ON m.id = r.interest_id
    WHERE r.monthly_rank = 1
), history AS (
    SELECT month_date,
           interest_name,
           max_index_composition,
           AVG(max_index_composition) OVER (
               ORDER BY month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
           ) AS three_month_rolling_avg,
           LAG(interest_name, 1) OVER (ORDER BY month_date) AS one_month_ago_interest_name,
           LAG(max_index_composition, 1) OVER (ORDER BY month_date) AS one_month_ago_max_index_composition,
           LAG(interest_name, 2) OVER (ORDER BY month_date) AS two_months_ago_interest_name,
           LAG(max_index_composition, 2) OVER (ORDER BY month_date) AS two_months_ago_max_index_composition
    FROM monthly_top
)
SELECT month_date,
       interest_name,
       ROUND(max_index_composition, 2) AS max_index_composition,
       ROUND(three_month_rolling_avg, 2) AS three_month_rolling_avg,
       one_month_ago_interest_name,
       ROUND(one_month_ago_max_index_composition, 2) AS one_month_ago_max_index_composition,
       two_months_ago_interest_name,
       ROUND(two_months_ago_max_index_composition, 2) AS two_months_ago_max_index_composition
FROM history
WHERE month_date >= '2018-09-01' AND month_date < '2019-09-01'
ORDER BY month_date;
