SELECT *
FROM  t_customer b 
ON a.customer_id = b.id 
LEFT JOIN t_industry c
ON b.industry_id = c.id 
WHERE c.name IN('Hospitality');
/* Purpose: Retrieves all customers, keeping those in the 'Hospitality' industry, and potentially including customers without a matching industry.
Behavior:
Performs a LEFT JOIN, including all customers from t_customer.
Filters the results in the WHERE clause, keeping only rows where c.name is 'Hospitality'. This might exclude customers without a matching industry in t_industry.

 */

SELECT *
FROM  t_customer b 
ON a.customer_id = b.id 
LEFT JOIN t_industry c
ON b.industry_id = c.id 
AND c.name IN('Hospitality');

/* Purpose: Retrieves all customers in the 'Hospitality' industry, potentially excluding customers without a matching industry.
Behavior:
The condition c.name IN('Hospitality') is applied within the ON clause of the LEFT JOIN.
This effectively filters the results during the join itself, potentially excluding customers without a matching industry in t_industry.
 */
SELECT *
FROM  t_customer b 
ON a.customer_id = b.id 
LEFT JOIN
  (
          SELECT id FROM t_industry 
          WHERE name IN('Hospitality') 
         ) c
ON b.industry_id = c.id 
;

/* Purpose: Retrieves all customers, keeping those in the 'Hospitality' industry, and includes NULL values for customers without a matching industry.
Behavior:
Uses a subquery to pre-filter the t_industry table, selecting only industries with the name 'Hospitality'.
Performs a LEFT JOIN with this filtered subquery, ensuring that all customers from t_customer are included, even if they don't have a matching industry. */

Rows without a match will have NULL values for columns from the t_industry table.
Key Differences:

Filtering Approach: Query 1 filters after the join, while Query 2 filters during the join. Query 3 pre-filters the industry table before the join.
Handling Missing Matches: Query 1 and Query 2 might exclude customers without a matching industry, while Query 3 includes them with NULL values.
Potential Performance: Query 3 might be less efficient due to the subquery, but it guarantees inclusion of all customers.
Choosing the Right Query:

Use Query 1 if you want to potentially exclude customers without a matching industry and filter after the join.
Use Query 2 if you want to potentially exclude customers without a matching industry and filter during the join.
Use Query 3 if you want to include all customers, even those without a matching industry, and handle missing matches with NULL values.