WITH artist_sales AS (
    SELECT a.ArtistId, SUM(ii.Quantity) AS units_sold
    FROM invoice_items AS ii
    JOIN tracks AS t ON t.TrackId = ii.TrackId
    JOIN albums AS a ON a.AlbumId = t.AlbumId
    GROUP BY a.ArtistId
), best_selling_artists AS (
    SELECT ArtistId
    FROM artist_sales
    WHERE units_sold = (SELECT MAX(units_sold) FROM artist_sales)
)
SELECT c.FirstName,
       ROUND(SUM(ii.UnitPrice * ii.Quantity), 2) AS AmountSpent
FROM customers AS c
JOIN invoices AS i ON i.CustomerId = c.CustomerId
JOIN invoice_items AS ii ON ii.InvoiceId = i.InvoiceId
JOIN tracks AS t ON t.TrackId = ii.TrackId
JOIN albums AS a ON a.AlbumId = t.AlbumId
JOIN best_selling_artists AS b ON b.ArtistId = a.ArtistId
GROUP BY c.CustomerId, c.FirstName
HAVING AmountSpent < 1.00
ORDER BY c.FirstName, c.CustomerId;
