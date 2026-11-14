WITH yash_chopra_id AS (
  SELECT id FROM Person WHERE name = 'Yash Chopra'
), 
actor_director_films AS (
  SELECT
    mc.person_id AS actor_id,
    md.director_id,
    COUNT(DISTINCT mc.movie_id) AS film_count
  FROM M_Cast mc
  JOIN M_Director md ON mc.movie_id = md.movie_id
  WHERE mc.role = 'Actor'
  GROUP BY mc.person_id, md.director_id
), 
max_films_per_actor AS (
  SELECT
    actor_id,
    MAX(film_count) AS max_films
  FROM actor_director_films
  GROUP BY actor_id
),
yash_chopra_actors AS (
  SELECT
    adf.actor_id,
    adf.film_count
  FROM actor_director_films adf
  JOIN yash_chopra_id yc ON adf.director_id = yc.id
),
actors_only_yash_max AS (
  SELECT
    yca.actor_id
  FROM yash_chopra_actors yca
  JOIN max_films_per_actor mfa ON yca.actor_id = mfa.actor_id AND yca.film_count = mfa.max_films
)
SELECT COUNT(*) AS actors_with_more_films_with_yash_chopra
FROM actors_only_yash_max aym
JOIN actor_director_films adf 
  ON aym.actor_id = adf.actor_id
GROUP BY aym.actor_id
HAVING COUNT(CASE WHEN adf.director_id <> (SELECT id FROM Person WHERE name = 'Yash Chopra') AND adf.film_count = (SELECT film_count FROM yash_chopra_actors WHERE actor_id = aym.actor_id) THEN 1 END) = 0
