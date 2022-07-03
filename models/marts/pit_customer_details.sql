SELECT *
  FROM (SELECT customer.customer_pk,
               customer_details.first_name,
               customer_details.last_name,
               customer_details.email,
               customer_details.effective_from,
               COALESCE(LEAD(effective_from)
                             OVER (PARTITION BY customer.customer_pk ORDER BY customer_details.effective_from),
                        '9999-01-01') AS effective_to
          FROM {{ ref('hub_customer') }} AS customer
                  LEFT JOIN {{ ref('sat_customer_details') }} AS customer_details
            ON customer.customer_pk = customer_details.customer_pk) AS details_history


