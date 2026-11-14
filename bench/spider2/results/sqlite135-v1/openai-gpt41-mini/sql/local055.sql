WITH ArtistSales AS (
    SELECT
        ar.artistid,
        ar.name AS artist_name,
        SUM(ii.unitprice * ii.quantity) AS total_sales
    FROM artists ar
    JOIN albums al ON ar.artistid = al.artistid
    JOIN tracks t ON al.albumid = t.albumid
    JOIN invoice_items ii ON t.trackid = ii.trackid
    GROUP BY ar.artistid, ar.name
),

RankedArtists AS (
    SELECT
        artistid,
        artist_name,
        total_sales,
        RANK() OVER (ORDER BY total_sales DESC, artist_name ASC) AS rank_top,
        RANK() OVER (ORDER BY total_sales ASC, artist_name ASC) AS rank_bottom
    FROM ArtistSales
),

TargetArtists AS (
    SELECT artistid, artist_name, total_sales FROM RankedArtists WHERE rank_top = 1 OR rank_bottom = 1
),

CustomerSpending AS (
    SELECT
        c.customerid,
        c.firstname || ' ' || c.lastname AS customer_name,
        ta.artist_name,
        SUM(ii.unitprice * ii.quantity) AS artist_spending
    FROM customers c
    JOIN invoices iv ON c.customerid = iv.customerid
    JOIN invoice_items ii ON iv.invoiceid = ii.invoiceid
    JOIN tracks t ON ii.trackid = t.trackid
    JOIN albums al ON t.albumid = al.albumid
    JOIN TargetArtists ta ON al.artistid = ta.artistid
    GROUP BY c.customerid, ta.artist_name
),

AvgSpending AS (
    SELECT
        artist_name,
        AVG(artist_spending) AS avg_spending
    FROM CustomerSpending
    GROUP BY artist_name
),

TopArtist AS (
    SELECT artist_name FROM RankedArtists WHERE rank_top = 1
),

BottomArtist AS (
    SELECT artist_name FROM RankedArtists WHERE rank_bottom = 1
)

SELECT 
    ABS(
        (SELECT avg_spending FROM AvgSpending WHERE artist_name = (SELECT artist_name FROM TopArtist)) -
        (SELECT avg_spending FROM AvgSpending WHERE artist_name = (SELECT artist_name FROM BottomArtist))
    ) AS absolute_difference
;
