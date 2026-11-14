WITH Top10Customers AS (
  SELECT customer_id
  FROM payment
  GROUP BY customer_id
  ORDER BY SUM(amount) DESC
  LIMIT 10
),
MonthlyPayments AS (
  SELECT
    customer_id,
    strftime('%Y-%m', payment_date) AS month,
    ROUND(SUM(amount), 2) AS monthly_amount
  FROM payment
  WHERE customer_id IN (SELECT customer_id FROM Top10Customers)
  GROUP BY customer_id, month
),
MonthlyDiffs AS (
  SELECT
    m1.customer_id,
    m1.month,
    ROUND(ABS(m1.monthly_amount - m2.monthly_amount), 2) AS diff
  FROM MonthlyPayments m1
  JOIN MonthlyPayments m2
    ON m1.customer_id = m2.customer_id
    AND strftime('%Y-%m', date(m2.month || '-01', '+1 month')) = m1.month
)
SELECT customer_id, month, diff
FROM MonthlyDiffs
ORDER BY diff DESC
LIMIT 1;
