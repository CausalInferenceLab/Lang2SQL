WITH HighRatedMovies AS (
    SELECT m.movie_id, m.title
    FROM movies m
    JOIN ratings r ON m.id = r.movie_id
    WHERE r.rating > 8
),
GenreCounts AS (
    SELECT g.genre, COUNT(DISTINCT hm.movie_id) AS movie_count
    FROM genre g
    JOIN HighRatedMovies hm ON g.movie_id = hm.movie_id
    GROUP BY g.genre
    ORDER BY movie_count DESC
    LIMIT 3
),
TopGenres AS (
    SELECT genre FROM GenreCounts
),
TopGenreMovies AS (
    SELECT hm.movie_id
    FROM HighRatedMovies hm
    JOIN genre g ON hm.movie_id = g.movie_id
    WHERE g.genre IN (SELECT genre FROM TopGenres)
),
DirectorFilmCounts AS (
    SELECT dm.director_id, COUNT(DISTINCT dm.movie_id) AS num_films
    FROM director_mapping dm
    WHERE dm.movie_id IN (SELECT movie_id FROM TopGenreMovies)
    GROUP BY dm.director_id
    ORDER BY num_films DESC
    LIMIT 4
)
SELECT n.name AS director_name, dfc.num_films
FROM DirectorFilmCounts dfc
JOIN names n ON dfc.director_id = n.id
ORDER BY dfc.num_films DESC;
