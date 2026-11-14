WITH EntertainerStylesRanked AS (
    SELECT
        EntertainerID,
        StyleID,
        StyleStrength,
        ROW_NUMBER() OVER (PARTITION BY EntertainerID ORDER BY StyleStrength DESC) AS rn
    FROM Entertainer_Styles
    WHERE StyleStrength <= 3
),
CustomerPrefsRanked AS (
    SELECT
        CustomerID,
        StyleID,
        PreferenceSeq,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY PreferenceSeq) AS rn
    FROM Musical_Preferences
),
EntertainerStylePairs AS (
    SELECT
        es1.EntertainerID,
        es1.StyleID AS style1,
        es2.StyleID AS style2
    FROM EntertainerStylesRanked es1
    JOIN EntertainerStylesRanked es2
      ON es1.EntertainerID = es2.EntertainerID
     AND es1.rn = 1
     AND es2.rn = 2
),
CustomerPrefPairs AS (
    SELECT
        c.CustomerID,
        c.StyleID AS style1,
        c2.StyleID AS style2
    FROM CustomerPrefsRanked c
    JOIN CustomerPrefsRanked c2
      ON c.CustomerID = c2.CustomerID
     AND c.rn = 1
     AND c2.rn = 2
),
MatchingPairs AS (
    SELECT es.EntertainerID, c.CustomerID
    FROM EntertainerStylePairs es
    JOIN CustomerPrefPairs c
      ON (es.style1 = c.style1 AND es.style2 = c.style2)
         OR (es.style1 = c.style2 AND es.style2 = c.style1)
)
SELECT
    e.EntStageName AS entertainer_stage_name,
    cu.CustLastName AS customer_last_name
FROM MatchingPairs m
JOIN Entertainers e ON m.EntertainerID = e.EntertainerID
JOIN Customers cu ON m.CustomerID = cu.CustomerID
LIMIT 1000;
