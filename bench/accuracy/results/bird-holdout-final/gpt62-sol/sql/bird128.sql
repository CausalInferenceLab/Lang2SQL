SELECT ROUND(AVG(u.Age), 2) AS average_age
FROM users AS u
WHERE u.Id IN (
    SELECT c.UserId
    FROM comments AS c
    WHERE INSTR(c.Text, 'http://') > 0
);
