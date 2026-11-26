WITH MatchingPairs AS (
    SELECT DISTINCT es1.EntertainerID, mp1.CustomerID
    FROM Entertainer_Styles AS es1
    JOIN Entertainer_Styles AS es2
        ON es2.EntertainerID = es1.EntertainerID
        AND es2.StyleStrength = 2
    JOIN Musical_Preferences AS mp1
        ON mp1.PreferenceSeq = 1
        AND mp1.StyleID IN (es1.StyleID, es2.StyleID)
    JOIN Musical_Preferences AS mp2
        ON mp2.CustomerID = mp1.CustomerID
        AND mp2.PreferenceSeq = 2
    WHERE es1.StyleStrength = 1
        AND (
            (mp1.StyleID = es1.StyleID AND mp2.StyleID = es2.StyleID)
            OR (mp1.StyleID = es2.StyleID AND mp2.StyleID = es1.StyleID)
        )
        AND es1.EntertainerID IN (
            SELECT EntertainerID
            FROM Entertainer_Styles
            GROUP BY EntertainerID
            HAVING COUNT(*) <= 3
        )
        AND mp1.CustomerID IN (
            SELECT CustomerID
            FROM Musical_Preferences
            GROUP BY CustomerID
            HAVING COUNT(*) <= 3
        )
)
SELECT e.EntStageName, c.CustLastName
FROM MatchingPairs AS p
JOIN Entertainers AS e ON e.EntertainerID = p.EntertainerID
JOIN Customers AS c ON c.CustomerID = p.CustomerID;
