SELECT COUNT(DISTINCT CDSCode) AS school_count
FROM frpm
WHERE "County Name" = 'Los Angeles'
  AND "Charter School (Y/N)" = 0
  AND "Percent (%) Eligible Free (K-12)" < 0.0018;
