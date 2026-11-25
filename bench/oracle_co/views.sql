-- Apply only after installing Oracle's unmodified CO sample schema.
-- Demo-specific business views, not part of the Oracle sample distribution.
CREATE VIEW co.l2s_order_count AS
SELECT order_id, CAST(order_tms AS DATE) AS order_day, 1 AS order_count FROM co.orders;
CREATE VIEW co.l2s_sales AS
SELECT o.order_id, CAST(o.order_tms AS DATE) AS order_day,
       o.order_status, SUM(i.unit_price*i.quantity) AS amount
FROM co.orders o JOIN co.order_items i ON i.order_id=o.order_id
GROUP BY o.order_id,o.order_tms,o.order_status;
CREATE VIEW co.l2s_finance_sales AS
SELECT o.order_id, CAST(o.order_tms AS DATE) AS order_day,
       SUM(i.unit_price*i.quantity) AS amount
FROM co.orders o JOIN co.order_items i ON i.order_id=o.order_id
WHERE o.order_status IN ('COMPLETE','REFUNDED')
GROUP BY o.order_id,o.order_tms;
