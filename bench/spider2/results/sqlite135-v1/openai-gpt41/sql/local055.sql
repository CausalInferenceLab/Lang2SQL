WITH artist_sales AS (
  SELECT 
    a.ArtistId, 
    a.Name AS ArtistName, 
    SUM(ii.UnitPrice * ii.Quantity) AS TotalSales
  FROM artists a
  JOIN albums al ON a.ArtistId = al.ArtistId
  JOIN tracks t ON al.AlbumId = t.AlbumId
  JOIN invoice_items ii ON t.TrackId = ii.TrackId
  GROUP BY a.ArtistId, a.Name
),
top_artist AS (
  SELECT ArtistId, ArtistName
  FROM artist_sales
  ORDER BY TotalSales DESC, ArtistName ASC
  LIMIT 1
),
low_artist AS (
  SELECT ArtistId, ArtistName
  FROM artist_sales
  ORDER BY TotalSales ASC, ArtistName ASC
  LIMIT 1
),
target_artists AS (
  SELECT ArtistId, ArtistName, 'top' AS RankFlag FROM top_artist
  UNION ALL
  SELECT ArtistId, ArtistName, 'low' AS RankFlag FROM low_artist
),
customer_spending AS (
  SELECT 
    c.CustomerId,
    ta.ArtistName,
    ta.RankFlag,
    SUM(ii.UnitPrice * ii.Quantity) AS AmountSpent
  FROM customers c
  JOIN invoices i ON c.CustomerId = i.CustomerId
  JOIN invoice_items ii ON i.InvoiceId = ii.InvoiceId
  JOIN tracks t ON ii.TrackId = t.TrackId
  JOIN albums al ON t.AlbumId = al.AlbumId
  JOIN target_artists ta ON al.ArtistId = ta.ArtistId
  GROUP BY c.CustomerId, ta.ArtistName, ta.RankFlag
)
SELECT
  ABS(
    (SELECT AVG(AmountSpent) FROM customer_spending WHERE RankFlag = 'top')
    - 
    (SELECT AVG(AmountSpent) FROM customer_spending WHERE RankFlag = 'low')
  ) AS AvgSpendingDifference
;
