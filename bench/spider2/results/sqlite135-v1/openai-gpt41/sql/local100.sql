WITH shahrukh AS (
  SELECT Person.id AS shahrukh_id
  FROM Person
  WHERE Person.name = 'Shahrukh Khan'
),
direct_costars AS (
  SELECT DISTINCT M_Cast.person_id AS costar_id
  FROM M_Cast
  JOIN M_Cast mc2 ON M_Cast.movie_id = mc2.movie_id
  JOIN shahrukh ON mc2.person_id = shahrukh.shahrukh_id
  WHERE M_Cast.person_id <> shahrukh.shahrukh_id
),
second_degree AS (
  SELECT DISTINCT M_Cast.person_id AS actor_id
  FROM M_Cast
  JOIN M_Cast mc2 ON M_Cast.movie_id = mc2.movie_id
  WHERE mc2.person_id IN (SELECT costar_id FROM direct_costars)
    AND M_Cast.person_id NOT IN (SELECT costar_id FROM direct_costars)
    AND M_Cast.person_id NOT IN (SELECT shahrukh_id FROM shahrukh)
)
SELECT COUNT(DISTINCT actor_id) AS shahrukh_number_2_count
FROM second_degree;
