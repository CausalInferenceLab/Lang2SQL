WITH English_Subjects AS (
    SELECT SubjectID
    FROM Subjects
    WHERE Name LIKE '%English%'
),
Completed_English_Classes AS (
    SELECT scs.StudentID, scs.Grade
    FROM Student_Class_Status scs
    JOIN Classes c ON scs.ClassID = c.ClassID
    WHERE scs.ClassStatus = 2
      AND c.SubjectID IN (SELECT SubjectID FROM English_Subjects)
),
Ranked_Students AS (
    SELECT
        s.LastName,
        ce.Grade,
        (
            SELECT COUNT(DISTINCT ce2.StudentID)
            FROM Completed_English_Classes ce2
            WHERE ce2.Grade >= ce.Grade
        ) AS Rank,
        (SELECT COUNT(DISTINCT StudentID) FROM Completed_English_Classes) AS Total
    FROM Completed_English_Classes ce
    JOIN Students s ON ce.StudentID = s.StudentID
),
Quintiled_Students AS (
    SELECT
        LastName,
        Grade,
        Rank,
        Total,
        CASE
            WHEN 1.0*Rank/Total <= 0.2 THEN 'First'
            WHEN 1.0*Rank/Total <= 0.4 THEN 'Second'
            WHEN 1.0*Rank/Total <= 0.6 THEN 'Third'
            WHEN 1.0*Rank/Total <= 0.8 THEN 'Fourth'
            ELSE 'Fifth'
        END AS Quintile
    FROM Ranked_Students
)
SELECT LastName, Quintile
FROM Quintiled_Students
ORDER BY
    CASE Quintile
        WHEN 'First' THEN 1
        WHEN 'Second' THEN 2
        WHEN 'Third' THEN 3
        WHEN 'Fourth' THEN 4
        WHEN 'Fifth' THEN 5
    END, LastName;
