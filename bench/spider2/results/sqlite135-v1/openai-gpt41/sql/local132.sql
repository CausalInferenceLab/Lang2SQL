SELECT
    E.stage_name,
    C.last_name
FROM
    Entertainers E
JOIN (
    SELECT
        entertainer_id,
        GROUP_CONCAT(style ORDER BY id) AS styles,
        COUNT(*) AS num_styles,
        MAX(CASE WHEN id = 1 THEN style END) AS style1,
        MAX(CASE WHEN id = 2 THEN style END) AS style2
    FROM (
        SELECT
            entertainer_id,
            style,
            ROW_NUMBER() OVER (PARTITION BY entertainer_id ORDER BY id) AS id
        FROM Entertainer_Styles
    )
    GROUP BY entertainer_id
    HAVING num_styles <= 3
) ES ON E.id = ES.entertainer_id
JOIN (
    SELECT
        customer_id,
        GROUP_CONCAT(preference ORDER BY id) AS preferences,
        COUNT(*) AS num_prefs,
        MAX(CASE WHEN id = 1 THEN preference END) AS pref1,
        MAX(CASE WHEN id = 2 THEN preference END) AS pref2
    FROM (
        SELECT
            customer_id,
            preference,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS id
        FROM Musical_Preferences
    )
    GROUP BY customer_id
    HAVING num_prefs <= 3
) CP ON 1=1
JOIN Customers C ON CP.customer_id = C.id
WHERE 
    ((ES.style1 = CP.pref1 AND ES.style2 = CP.pref2) OR
     (ES.style1 = CP.pref2 AND ES.style2 = CP.pref1))
