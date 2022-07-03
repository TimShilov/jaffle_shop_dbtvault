{{ config(materialized='view') }}

SELECT CAST(DATE_TRUNC('week', order_details.order_date) AS DATE) AS order_week,
       order_details.status,
       COUNT(orders.order_pk) AS count
  FROM {{ ref('hub_order') }} AS orders
	LEFT JOIN {{ ref('sat_order_details') }} AS order_details
    ON order_details.order_pk = orders.order_pk
 GROUP BY order_week, status
 ORDER BY order_week DESC
