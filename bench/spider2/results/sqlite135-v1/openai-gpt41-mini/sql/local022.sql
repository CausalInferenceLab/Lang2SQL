WITH team_lost AS (
    SELECT match_id, loser
    FROM match
),
player_runs_in_match AS (
    SELECT pm.match_id, pm.player_id, pm.team_id, SUM(bs.runs) AS total_runs
    FROM player_match pm
    JOIN batsman_scored bs ON pm.match_id = bs.match_id AND pm.player_id = bs.striker
    GROUP BY pm.match_id, pm.player_id, pm.team_id
),
players_scored_100_in_losing_team AS (
    SELECT p.name
    FROM player_runs_in_match pr
    JOIN team_lost tl ON pr.match_id = tl.match_id AND pr.team_id = tl.loser
    JOIN player p ON pr.player_id = p.id
    WHERE pr.total_runs >= 100
)
SELECT DISTINCT name
FROM players_scored_100_in_losing_team
;
