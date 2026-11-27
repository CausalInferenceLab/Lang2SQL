SELECT d.forename,
       d.surname,
       d.url,
       d.dob,
       COUNT(NULLIF(TRIM(q.q3), '')) AS q3_count
FROM drivers AS d
LEFT JOIN qualifying AS q ON q.driverId = d.driverId
WHERE d.nationality = 'German'
  AND d.dob >= '1971-01-01'
  AND d.dob < '1986-01-01'
GROUP BY d.driverId, d.forename, d.surname, d.url, d.dob
ORDER BY q3_count DESC, d.dob ASC;
