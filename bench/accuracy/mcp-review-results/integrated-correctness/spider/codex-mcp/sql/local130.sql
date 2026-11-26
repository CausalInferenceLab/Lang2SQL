WITH ranked_grades AS (
    SELECT
        st.StudLastName,
        CUME_DIST() OVER (ORDER BY ss.Grade DESC) AS grade_share
    FROM Student_Schedules AS ss
    JOIN Students AS st ON st.StudentID = ss.StudentID
    JOIN Classes AS c ON c.ClassID = ss.ClassID
    JOIN Subjects AS sub ON sub.SubjectID = c.SubjectID
    WHERE ss.ClassStatus = 2
      AND sub.CategoryID = 'ENG'
)
SELECT
    StudLastName,
    CASE
        WHEN grade_share <= 0.20 THEN 'First'
        WHEN grade_share <= 0.40 THEN 'Second'
        WHEN grade_share <= 0.60 THEN 'Third'
        WHEN grade_share <= 0.80 THEN 'Fourth'
        ELSE 'Fifth'
    END AS Quintile
FROM ranked_grades
ORDER BY grade_share, StudLastName
LIMIT 1000
