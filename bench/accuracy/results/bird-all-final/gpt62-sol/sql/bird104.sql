SELECT SUM(convertedManaCost) AS total_converted_mana_cost
FROM cards
WHERE artist = 'Rob Alexander'
  AND originalReleaseDate >= '2013-01-01'
  AND originalReleaseDate < '2014-01-01';
