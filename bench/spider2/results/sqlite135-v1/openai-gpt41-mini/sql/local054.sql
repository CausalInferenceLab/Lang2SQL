WITH best_selling_artist AS (
    SELECT ar.artistid
    FROM artists ar
    JOIN albums al ON al.artistid = ar.artistid
    JOIN tracks t ON t.albumid = al.albumid
    JOIN invoice_items ii ON ii.trackid = t.trackid
    GROUP BY ar.artistid
    ORDER BY SUM(ii.unitprice * ii.quantity) DESC
    LIMIT 1
),
customer_spending AS (
    SELECT c.customerid, c.firstname, SUM(ii.unitprice * ii.quantity) AS total_spent
    FROM customers c
    JOIN invoices i ON i.customerid = c.customerid
    JOIN invoice_items ii ON ii.invoiceid = i.invoiceid
    JOIN tracks t ON t.trackid = ii.trackid
    JOIN albums al ON al.albumid = t.albumid
    WHERE al.artistid = (SELECT artistid FROM best_selling_artist)
    GROUP BY c.customerid, c.firstname
)
SELECT firstname, total_spent
FROM customer_spending
WHERE total_spent < 1;
