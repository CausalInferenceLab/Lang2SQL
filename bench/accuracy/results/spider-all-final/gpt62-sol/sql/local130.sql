WITH completed_english AS (
    SELECT st.StudentID, st.StudLastName, ss.ClassID, ss.Grade
    FROM Student_Schedules AS ss
    JOIN Students AS st ON st.StudentID = ss.StudentID
    JOIN Classes AS c ON c.ClassID = ss.ClassID
    JOIN Subjects AS s ON s.SubjectID = c.SubjectID
    WHERE ss.ClassStatus = 2
      AND s.CategoryID = 'ENG'
), student_total AS (
    SELECT COUNT(DISTINCT StudentID) AS student_count
    FROM completed_english
), ranked AS (
    SELECT e.*,
           1.0 * (
               SELECT COUNT(DISTINCT peer.StudentID)
               FROM completed_english AS peer
               WHERE peer.Grade >= e.Grade
           ) / t.student_count AS rank_fraction
    FROM completed_english AS e
    CROSS JOIN student_total AS t
)
SELECT StudLastName,
       CASE
           WHEN Grade IS NULL THEN NULL
           WHEN rank_fraction <= 0.20 THEN 'First'
           WHEN rank_fraction <= 0.40 THEN 'Second'
           WHEN rank_fraction <= 0.60 THEN 'Third'
           WHEN rank_fraction <= 0.80 THEN 'Fourth'
           ELSE 'Fifth'
       END AS Quintile
FROM ranked
ORDER BY Grade IS NULL, rank_fraction, Grade DESC, StudLastName, StudentID, ClassID;
