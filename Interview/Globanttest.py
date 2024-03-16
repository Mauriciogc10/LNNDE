# ## US AIRLINE QUESTION
# # US Airline passengers have become frustrated at the fact that, after finishing one movie in their flight from SF -> NEWARK
# # their in-flight entertainment will suggest movies that are too long for them to finish

# # Build a function that takes in flight_time (integer), and a dictionary of {movie_name: length_in_mins}, and returns 
# # all 2-movie combinations that can be watched within the flight time

# 1. Make sure there's no duplicates
# 2. Provide a list of tuples as shown below, where the movie combinations are sorted and we show total movie time for that combination
# 3. Don't list the same movies 

# expected output -> [ (('movie_a', 'movie_b'), 225), (('movie_a', 'movie_c'), 245)) ]

#[tuple(tuple(movie1,movie2),duration), .. . .. . .]


def find_movie_combinations(flight_time, movies):
    valid_combinations = []
    
    for movie1, duration1 in movies.items():
        for movie2, duration2 in movies.items():
            if movie1 != movie2 and duration1 + duration2 <= flight_time:
                combination = (movie1, movie2)
                total_duration = duration1 + duration2
                if total_duration <= flight_time and combination not in valid_combinations:
                    valid_combinations.append((combination, total_duration))

    return valid_combinations

flight_time = 265
movies = {
  'movie_a': 105, 'movie_b': 120, 'movie_c': 140, 'movie_d': 145, 'movie_e': 150,
  'movie_f': 155, 'movie_g': 160, 'movie_h': 165, 'movie_i': 170, 'movie_j': 90, 'movie_k': 145, 'movie_l': 145
}

combinations = find_movie_combinations(flight_time, movies)
print(combinations)

"""
SQL test


could you please use these 2 tables and create the output
TABLE A
ID | VAL
10 | A
20 | BB
30 | CC


TABLE B
ID | VAL
20 | BB
30 | CC
40 | DD
10 | 


OUTPUT
ID | VAL | MSG
10 | A   | I BELONG TO TABLE A
40 | DD  | I BELONG TO TABLE B
30 | cc  | I inside two tables
"""

SELECT
    COALESCE(A.ID, B.ID) AS ID,
    COALESCE(UPPER(A.VAL), UPPER(B.VAL)) AS VAL,
    CASE
        WHEN A.ID IS NOT NULL AND B.ID IS NOT NULL THEN 'I inside two tables'
        WHEN A.ID IS NOT NULL THEN 'I BELONG TO TABLE A'
        WHEN B.ID IS NOT NULL THEN 'I BELONG TO TABLE B'
    END AS MSG
FROM
    TABLE_A A
FULL OUTER JOIN
    TABLE_B B
ON
    A.ID = B.ID;



