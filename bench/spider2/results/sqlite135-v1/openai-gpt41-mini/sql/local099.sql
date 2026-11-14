WITH DirectorFilms AS (
    SELECT
        M_Cast.PID AS actor_pid,
        M_Director.PID AS director_pid,
        COUNT(*) AS film_count
    FROM M_Cast
    JOIN Movie ON M_Cast.MID = Movie.MID
    JOIN M_Director ON Movie.MID = M_Director.MID
    GROUP BY M_Cast.PID, M_Director.PID
),
YashChopra AS (
    SELECT PID FROM Person WHERE Name = 'Yash Chopra'
),
YashChopraFilms AS (
    SELECT actor_pid, film_count
    FROM DirectorFilms
    WHERE director_pid = (SELECT PID FROM YashChopra)
),
ActorsMoreWithYashChopra AS (
    SELECT df.actor_pid
    FROM DirectorFilms df
    JOIN YashChopraFilms yf ON df.actor_pid = yf.actor_pid
    WHERE yf.film_count > df.film_count
      AND df.director_pid != (SELECT PID FROM YashChopra)
    GROUP BY df.actor_pid
    HAVING COUNT(*) = (SELECT COUNT(*) FROM DirectorFilms df2 WHERE df2.actor_pid = df.actor_pid AND df2.director_pid != (SELECT PID FROM YashChopra))
)
SELECT COUNT(*) AS actors_more_films_with_yash_chopra
FROM ActorsMoreWithYashChopra;
