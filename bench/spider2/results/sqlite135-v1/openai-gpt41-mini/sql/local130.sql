WITH CompletedEnglish AS (
    SELECT
        s.LastName,
        scs.Grade
    FROM Students s
    JOIN Student_Class_Status scs ON s.ID = scs.StudentID
    JOIN Classes c ON scs.ClassID = c.ID
    JOIN Subjects subj ON c.SubjectID = subj.ID
    WHERE subj.Name = 'English' AND scs.ClassStatus = 2
),
Ranked AS (
    SELECT
        LastName,
        Grade,
        (SELECT COUNT(*) FROM CompletedEnglish ce2 WHERE ce2.Grade >= ce1.Grade) AS GradeRank,
        (SELECT COUNT(*) FROM CompletedEnglish) AS TotalStudents
    FROM CompletedEnglish ce1
),
Quintiles AS (
  SELECT
    LastName,
    Grade,
    GradeRank,
    TotalStudents,
    CASE
      WHEN GradeRank <= TotalStudents * 0.2 THEN 'First'
      WHEN GradeRank <= TotalStudents * 0.4 THEN 'Second'
      WHEN GradeRank <= TotalStudents * 0.6 THEN 'Third'
      WHEN GradeRank <= TotalStudents * 0.8 THEN 'Fourth'
      ELSE 'Fifth'
    END AS Quintile
  FROM Ranked
)
SELECT LastName, Quintile
FROM Quintiles
ORDER BY 
  CASE Quintile
    WHEN 'First' THEN 1
    WHEN 'Second' THEN 2
    WHEN 'Third' THEN 3
    WHEN 'Fourth' THEN 4
    WHEN 'Fifth' THEN 5
  END;
