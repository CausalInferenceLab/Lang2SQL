WITH TrackDurations AS (
  SELECT
    TrackId,
    Milliseconds / 60000.0 AS DurationMin
  FROM Track
),
Stats AS (
  SELECT
    MIN(DurationMin) AS MinDuration,
    AVG(DurationMin) AS AvgDuration,
    MAX(DurationMin) AS MaxDuration
  FROM TrackDurations
),
Classified AS (
  SELECT
    t.TrackId,
    t.DurationMin,
    CASE
      WHEN t.DurationMin <= (s.MinDuration + s.AvgDuration) / 2 THEN 'Short'
      WHEN t.DurationMin <= (s.AvgDuration + s.MaxDuration) / 2 THEN 'Medium'
      ELSE 'Long'
    END AS LengthType
  FROM TrackDurations t, Stats s
),
RevenueByTrack AS (
  SELECT
    il.TrackId,
    SUM(il.UnitPrice * il.Quantity) AS Revenue
  FROM InvoiceLine il
  GROUP BY il.TrackId
),
Summary AS (
  SELECT
    c.LengthType,
    MIN(c.DurationMin) AS MinDuration,
    MAX(c.DurationMin) AS MaxDuration,
    SUM(COALESCE(r.Revenue, 0)) AS TotalRevenue
  FROM Classified c
  LEFT JOIN RevenueByTrack r ON c.TrackId = r.TrackId
  GROUP BY c.LengthType
)
SELECT LengthType, ROUND(MinDuration, 2) AS MinDuration_Minutes, ROUND(MaxDuration, 2) AS MaxDuration_Minutes, ROUND(TotalRevenue, 2) AS TotalRevenue
FROM Summary
ORDER BY CASE LengthType WHEN 'Short' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Long' THEN 3 END
LIMIT 1000;
