#########other examples
###input
+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
|passengerid|survived|pclass|                name|   sex| age|sibsp|parch|          ticket|   fare|cabin|embarked|
+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
|          1|       0|     3|Braund, Mr. Owen ...|  male|22.0|    1|    0|       A/5 21171|   7.25| null|       S|
|          2|       1|     1|Cumings, Mrs. Joh...|female|38.0|    1|    0|        PC 17599|71.2833|  C85|       C|
|          3|       1|     3|Heikkinen, Miss. ...|female|26.0|    0|    0|STON/O2. 3101282|  7.925| null|       S|
|          4|       1|     1|Futrelle, Mrs. Ja...|female|35.0|    1|    0|          113803|   53.1| C123|       S|
|          5|       0|     3|Allen, Mr. Willia...|  male|35.0|    0|    0|          373450|   8.05| null|       S|
|          6|       0|     3|    Moran, Mr. James|  male|null|    0|    0|          330877| 8.4583| null|       Q|
|          7|       0|     1|McCarthy, Mr. Tim...|  male|54.0|    0|    0|           17463|51.8625|  E46|       S|
|          8|       0|     3|Palsson, Master. ...|  male| 2.0|    3|    1|          349909| 21.075| null|       S|
|          9|       1|     3|Johnson, Mrs. Osc...|female|27.0|    0|    2|          347742|11.1333| null|       S|
|         10|       1|     2|Nasser, Mrs. Nich...|female|14.0|    1|    0|          237736|30.0708| null|       C|
|         11|       1|     3|Sandstrom, Miss. ...|female| 4.0|    1|    1|         PP 9549|   16.7|   G6|       S|
|         12|       1|     1|Bonnell, Miss. El...|female|58.0|    0|    0|          113783|  26.55| C103|       S|
|         13|       0|     3|Saundercock, Mr. ...|  male|20.0|    0|    0|       A/5. 2151|   8.05| null|       S|
|         14|       0|     3|Andersson, Mr. An...|  male|39.0|    1|    5|          347082| 31.275| null|       S|
|         15|       0|     3|Vestrom, Miss. Hu...|female|14.0|    0|    0|          350406| 7.8542| null|       S|
|         16|       1|     2|Hewlett, Mrs. (Ma...|female|55.0|    0|    0|          248706|   16.0| null|       S|
|         17|       0|     3|Rice, Master. Eugene|  male| 2.0|    4|    1|          382652| 29.125| null|       Q|
|         18|       1|     2|Williams, Mr. Cha...|  male|null|    0|    0|          244373|   13.0| null|       S|
|         19|       0|     3|Vander Planke, Mr...|female|31.0|    1|    0|          345763|   18.0| null|       S|
|         20|       1|     3|Masselmani, Mrs. ...|female|null|    0|    0|            2649|  7.225| null|       C|
+-----------+--------+------+--------------------+------+----+-----+-----+----------------+-------+-----+--------+
####expected output
survived	first_class	second_class	third_class
0	        11	        6	            42
1	        10	        12	            19

# Import your libraries
import pyspark
from pyspark.sql.functions import *

# Start writing code
titanic

# To validate your solution, convert your final pySpark df to a pandas df
titanic.select(
    col("survived"),
    (when(col("pclass") == 1, 1).otherwise(0).alias("first_class")),
    (when(col("pclass") == 2, 1).otherwise(0).alias("second_class")),
    (when(col("pclass") == 3, 1).otherwise(0).alias("third_class"))
    ).groupBy(col("survived")).agg(
        sum(col("first_class")).alias("first_class"),
        sum(col("second_class")).alias("second_class"),
        sum(col("third_class")).alias("third_class")
        ).toPandas()

##### BIKES LAST USED #########

#Find the last time each bike was in use. Output both the bike number and the date-timestamp of the bike's last use (i.e., the date-time the bike was returned). Order the results by bikes that were most recently used.

+-------------+----------------+-------------------+--------------------+--------------+-------------------+--------------------+------------+-----------+----------+------+
|     duration|duration_seconds|         start_time|       start_station|start_terminal|           end_time|         end_station|end_terminal|bike_number|rider_type|    id|
+-------------+----------------+-------------------+--------------------+--------------+-------------------+--------------------+------------+-----------+----------+------+
|0h 10m 47sec.|             647|2012-03-25 10:30:00|17th & Corcoran S...|         31214|2012-03-25 10:40:00|Calvert St & Wood...|       31106|     W00576|Registered|326188|
|0h 11m 45sec.|             705|2012-03-28 18:59:00|Rosslyn Metro / W...|         31015|2012-03-28 19:11:00|      21st & M St NW|       31212|     W00011|Registered|345585|
| 0h 7m 45sec.|             465|2012-03-12 22:30:00|       3rd & H St NE|         31616|2012-03-12 22:37:00|Florida Ave & R S...|       31503|     W01215|Registered|251919|
| 0h 4m 27sec.|             267|2012-03-12 20:11:00|      14th & G St NW|         31238|2012-03-12 20:15:00|14th & Rhode Isla...|       31203|     W00455|Registered|251426|
| 0h 10m 2sec.|             602|2012-02-03 09:06:00|Lamont & Mt Pleas...|         31107|2012-02-03 09:16:00|17th & Rhode Isla...|       31239|     W00300|Registered|105965|
|0h 24m 59sec.|            1499|2012-03-30 19:35:00|Eastern Market Me...|         31613|2012-03-30 20:00:00|Massachusetts Ave...|       31200|     W01352|Registered|357661|
|0h 13m 45sec.|             825|2012-03-10 16:44:00|North Capitol St ...|         31624|2012-03-10 16:58:00|       Thomas Circle|       31241|     W00089|Registered|240483|
|0h 12m 58sec.|             778|2012-02-09 21:32:00|      14th & R St NW|         31202|2012-02-09 21:45:00|      18th & M St NW|       31221|     W01158|Registered|129535|
|  0h 2m 2sec.|             122|2012-03-29 17:36:00|      18th & Bell St|         31007|2012-03-29 17:38:00|   23rd & Crystal Dr|       31011|     W00653|Registered|350819|
|0h 17m 44sec.|            1064|2012-02-18 08:43:00|      21st & I St NW|         31205|2012-02-18 09:01:00|       8th & H St NW|       31228|     W01398|Registered|156324|
|0h 19m 43sec.|            1183|2012-03-31 19:08:00|  19th & E Street NW|         31206|2012-03-31 19:28:00|10th St & Constit...|       31219|     W01278|    Casual|363912|
|0h 16m 33sec.|             993|2012-02-22 08:20:00|7th & R St NW / S...|         31245|2012-02-22 08:37:00|New York Ave & 15...|       31222|     W00356|Registered|170556|
| 0h 6m 49sec.|             409|2012-03-22 17:10:00|Eastern Market Me...|         31613|2012-03-22 17:17:00|      13th & D St NE|       31622|     W00211|Registered|310589|
| 0h 5m 34sec.|             334|2012-01-19 20:39:00|Columbus Circle /...|         31623|2012-01-19 20:45:00|      13th & H St NE|       31611|     W00724|Registered| 56016|
|0h 14m 17sec.|             857|2012-03-31 10:30:00|      13th & D St NE|         31622|2012-03-31 10:44:00|       4th & E St SW|       31244|     W01006|Registered|359385|
|0h 16m 11sec.|             971|2012-03-14 17:24:00|13th St & New Yor...|         31227|2012-03-14 17:40:00|      11th & H St NE|       31614|     W00034|Registered|261560|
|0h 13m 42sec.|             822|2012-03-31 09:10:00|36th & Calvert St...|         31304|2012-03-31 09:24:00|       7th & T St NW|       31109|     W01242|Registered|359045|
|0h 30m 29sec.|            1829|2012-03-23 17:42:00| 11th & Kenyon St NW|         31102|2012-03-23 18:13:00|Van Ness Metro / UDC|       31300|     W00688|Registered|319246|
| 0h 9m 11sec.|             551|2012-03-29 15:10:00|       4th & M St SW|         31108|2012-03-29 15:20:00|USDA / 12th & Ind...|       31217|     W01330|Registered|349768|
|0h 11m 30sec.|             690|2012-01-05 08:23:00|14th & Rhode Isla...|         31203|2012-01-05 08:34:00|North Capitol St ...|       31624|     W00380|Registered|  9340|
+-------------+----------------+-------------------+--------------------+--------------+-------------------+--------------------+------------+-----------+----------+------+

# Import your libraries
import pyspark
from pyspark.sql.functions import col,max,desc

# Start writing code
dc_bikeshare_q1_2012 = dc_bikeshare_q1_2012.select(col("bike_number"),col("end_time"))

#dc_bikeshare_q1_2012.show()
answer_df= dc_bikeshare_q1_2012.groupby(col("bike_number")).agg(
    max(col("end_time")).alias("last_used"))
answer_df = answer_df.orderBy(col("last_used").desc())
answer_df.toPandas()