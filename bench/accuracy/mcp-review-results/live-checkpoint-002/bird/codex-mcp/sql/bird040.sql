WITH eligible AS (
    SELECT
        f.CDSCode,
        f."Enrollment (K-12)" AS enrollment,
        f."FRPM Count (K-12)" AS frpm_count,
        1.0 * f."FRPM Count (K-12)" / f."Enrollment (K-12)" AS frpm_share
    FROM frpm AS f
    WHERE f."Educational Option Type" = 'Traditional'
      AND f."Charter School (Y/N)" = 0
      AND f."Enrollment (K-12)" > 100
),
selected AS (
    SELECT 1 AS part_order, 'largest FRPM-eligible count' AS part, e.*
    FROM eligible AS e
    WHERE e.frpm_count = (SELECT MAX(frpm_count) FROM eligible)
    UNION ALL
    SELECT 2, 'smallest FRPM share', e.*
    FROM eligible AS e
    WHERE e.frpm_share = (SELECT MIN(frpm_share) FROM eligible)
    UNION ALL
    SELECT 3, 'the CDS code from my notes', e.*
    FROM eligible AS e
    WHERE e.CDSCode = '04615150435602'
)
SELECT
    p.part,
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
    t.AvgScrRead + t.AvgScrMath + t.AvgScrWrite AS average_total_sat_score,
    100.0 * t.NumGE1500 / NULLIF(t.NumTstTakr, 0) AS pct_sat_takers_scoring_1500_or_more,
    s.OpenDate AS opening_date,
    CASE WHEN s.ClosedDate IS NULL THEN 'open' ELSE 'closed' END AS status
FROM selected AS p
JOIN schools AS s ON s.CDSCode = p.CDSCode
LEFT JOIN satscores AS t ON t.cds = p.CDSCode
ORDER BY p.part_order, s.School, p.CDSCode
LIMIT -1
