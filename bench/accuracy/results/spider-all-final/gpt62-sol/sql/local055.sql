WITH customer_artist_spending AS (
    SELECT a.ArtistId,
           i.CustomerId,
           SUM(ii.UnitPrice * ii.Quantity) AS spending
    FROM invoice_items AS ii
    JOIN invoices AS i ON i.InvoiceId = ii.InvoiceId
    JOIN tracks AS t ON t.TrackId = ii.TrackId
    JOIN albums AS a ON a.AlbumId = t.AlbumId
    GROUP BY a.ArtistId, i.CustomerId
), artist_sales AS (
    SELECT ar.ArtistId,
           ar.Name,
           ROUND(SUM(cas.spending), 2) AS total_sales
    FROM customer_artist_spending AS cas
    JOIN artists AS ar ON ar.ArtistId = cas.ArtistId
    GROUP BY ar.ArtistId, ar.Name
), selected_artists AS (
    SELECT (
               SELECT ArtistId
               FROM artist_sales
               ORDER BY total_sales DESC, Name ASC, ArtistId ASC
               LIMIT 1
           ) AS top_artist_id,
           (
               SELECT ArtistId
               FROM artist_sales
               ORDER BY total_sales ASC, Name ASC, ArtistId ASC
               LIMIT 1
           ) AS bottom_artist_id
)
SELECT ABS(
           AVG(CASE WHEN cas.ArtistId = sa.top_artist_id THEN cas.spending END) -
           AVG(CASE WHEN cas.ArtistId = sa.bottom_artist_id THEN cas.spending END)
       ) AS absolute_average_spending_difference
FROM customer_artist_spending AS cas
JOIN selected_artists AS sa
  ON cas.ArtistId IN (sa.top_artist_id, sa.bottom_artist_id);
