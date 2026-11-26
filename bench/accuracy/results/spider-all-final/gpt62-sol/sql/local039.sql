SELECT c.name AS category
FROM rental AS r
JOIN inventory AS i ON i.inventory_id = r.inventory_id
JOIN film_category AS fc ON fc.film_id = i.film_id
JOIN category AS c ON c.category_id = fc.category_id
JOIN customer AS cu ON cu.customer_id = r.customer_id
JOIN address AS a ON a.address_id = cu.address_id
JOIN city AS ci ON ci.city_id = a.city_id
WHERE ci.city LIKE 'A%' OR ci.city LIKE '%-%'
GROUP BY c.category_id, c.name
ORDER BY SUM(strftime('%s', r.return_date) - strftime('%s', r.rental_date)) / 3600.0 DESC,
         c.name ASC
LIMIT 1;
