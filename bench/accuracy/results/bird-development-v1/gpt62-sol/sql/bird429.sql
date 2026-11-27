WITH patient_labs AS (
    SELECT p.ID, l.FG, TRIM(l."U-PRO") AS protein
    FROM Patient AS p
    JOIN Laboratory AS l ON l.ID = p.ID
    WHERE DATE(p.Birthday) > '1980-01-01'
), normalized AS (
    SELECT ID, FG, protein,
           CASE WHEN SUBSTR(protein, 1, 2) = '>='
                THEN TRIM(SUBSTR(protein, 3))
                ELSE protein
           END AS protein_mg_dl
    FROM patient_labs
)
SELECT COUNT(DISTINCT ID) AS eligible_patient_count
FROM normalized
WHERE FG < 150 OR FG > 450
   OR protein IN ('3', '4', '3+', '4+')
   OR (protein_mg_dl NOT GLOB '*[^0-9.]*'
       AND protein_mg_dl NOT GLOB '*.*.*'
       AND CAST(protein_mg_dl AS REAL) >= 300);
