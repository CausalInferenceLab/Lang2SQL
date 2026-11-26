WITH eligible AS (
    SELECT f.CDSCode,
           f."Enrollment (K-12)" AS enrollment,
           f."FRPM Count (K-12)" AS frpm_count,
           1.0 * f."FRPM Count (K-12)" / f."Enrollment (K-12)" AS frpm_share
    FROM frpm AS f
    WHERE f."Educational Option Type" = 'Traditional'
      AND f."Charter School (Y/N)" = 0
      AND f."Enrollment (K-12)" > 100
), extremes AS (
    SELECT MAX(frpm_count) AS max_count, MIN(frpm_share) AS min_share
    FROM eligible
), selected AS (
    SELECT 1 AS part_order, 'largest FRPM-eligible count' AS part, e.*
    FROM eligible AS e CROSS JOIN extremes AS x
    WHERE e.frpm_count = x.max_count
    UNION ALL
    SELECT 2, 'smallest FRPM share', e.*
    FROM eligible AS e CROSS JOIN extremes AS x
    WHERE e.frpm_share = x.min_share
    UNION ALL
    SELECT 3, 'the CDS code from my notes', e.*
    FROM eligible AS e
    WHERE e.CDSCode = '04615150435602'
)
SELECT p.part,
       s.School AS school_name,
       s.County AS county,
       s.City AS city,
       s.MailStreet AS mailing_street,
       s.MailCity AS mailing_city,
       s.MailState AS mailing_state,
       s.MailZip AS mailing_zip,
       s.Website AS website,
       p.enrollment AS k12_enrollment,
       p.frpm_count AS frpm_eligible_count,
       p.frpm_share,
       a.AvgScrRead + a.AvgScrMath + a.AvgScrWrite AS avg_total_sat_score,
       100.0 * a.NumGE1500 / NULLIF(a.NumTstTakr, 0) AS pct_sat_takers_scoring_1500_or_more,
       s.OpenDate AS opening_date,
       CASE WHEN NULLIF(s.ClosedDate, '') IS NOT NULL THEN 'closed' ELSE 'open' END AS status
FROM selected AS p
JOIN schools AS s ON s.CDSCode = p.CDSCode
LEFT JOIN satscores AS a ON a.cds = p.CDSCode AND a.rtype = 'S'
ORDER BY p.part_order, s.School, p.CDSCode;
