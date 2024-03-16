-- Uses WHERE clause after the full join
-- This filters the joined results based on c.name, meaning all rows from both t_customer and t_industry are joined first, then only those where c.name is "Hospitality" are kept.
-- This can be less efficient, especially if the dataset is large and many non-matching industries exist.
SELECT *
FROM  t_customer b 
ON a.customer_id = b.id 
LEFT JOIN t_industry c
ON b.industry_id = c.id 
WHERE c.name IN('Hospitality');


##########################################################
-- Uses AND clause within the LEFT JOIN on c.name
-- This filters the t_industry table before joining with t_customer, meaning only t_customer rows with matching industry_id (corresponding to "Hospitality" industries) are included in the join.
-- This is typically more efficient if the filter drastically reduces the number of rows in t_industry.
SELECT *
FROM  t_customer b 
ON a.customer_id = b.id 
LEFT JOIN t_industry c
ON b.industry_id = c.id 
AND c.name IN('Hospitality');

##########################################################
-- Utilizes a subquery to pre-filter the t_industry table by name
-- This creates a temporary table containing only IDs of "Hospitality" industries.
-- The join then uses this filtered version of t_industry for the join, similar to the second query.
-- This can be slightly less efficient than option 2 due to the extra subquery step, but may be clearer to read in certain situations.
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