from extract import (
    title_basics, 
    title_akas, 
    name_basics, 
    title_crew,
    title_episode, 
    title_principals,
    title_ratings
)
from pyspark.sql.functions import concat_ws
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrameStatFunctions as statF












title_akas = title_akas.withColumn("types", concat_ws(",", title_akas["types"]))
title_akas = title_akas.withColumn("attributes", concat_ws(",", title_akas["attributes"]))
name_basics = name_basics.withColumn("primaryProfession", concat_ws(",", name_basics["primaryProfession"]))
name_basics = name_basics.withColumn("knownForTitles", concat_ws(",", name_basics["knownForTitles"]))
title_crew = title_crew.withColumn("directors", concat_ws(",", title_crew["directors"]))
title_crew = title_crew.withColumn("writers", concat_ws(",", title_crew["writers"]))
title_basics = title_basics.withColumn("genres", concat_ws(",", title_basics["genres"]))

Write csv
title_basics.write.csv("/app/data/title_basics.csv", header=True, mode="overwrite") 
title_akas.write.csv("/app/data/title_akas.csv", header=True, mode="overwrite") 
name_basics.write.csv("/app/data/name_basics.csv", header=True, mode="overwrite") 
title_crew.write.csv("/app/data/title_crew.csv", header=True, mode="overwrite")
title_episode.write.csv("/app/data/title_episode.csv", header=True, mode="overwrite") 
title_principals.write.csv("/app/data/title_principals.csv", header=True, mode="overwrite")
title_ratings.write.csv("/app/data/title_ratings.csv", header=True, mode="overwrite")

comedy_movies = title_basics.filter(F.col("genres").contains("Comedy"))

rated_comedy_movies = title_ratings.join(comedy_movies, "tconst")

average_rating_by_movie = rated_comedy_movies.groupBy("tconst", "primaryTitle") \
    .agg(F.avg("averageRating").alias("avgRating"), F.sum("numVotes").alias("totalVotes"))

popular_comedy_movies = average_rating_by_movie.filter(F.col("totalVotes") >= 100)

windowSpec = Window.orderBy(F.desc("avgRating"))
ranked_comedy_movies = popular_comedy_movies.withColumn("rank", F.rank().over(windowSpec))

result_task_1 = ranked_comedy_movies.filter("rank = 1").select("primaryTitle", "avgRating", "totalVotes")

result_task_1.write.csv("/app/data/result_task_1.csv", header=True, mode="overwrite") 


thriller_movies = title_basics.filter((F.col("genres").contains("Thriller")) & (F.col("startYear") >= 1990))

average_runtime_trend = thriller_movies.groupBy("startYear").agg(F.avg("runtimeMinutes").alias("avgRuntime"))

result_task_2 = average_runtime_trend.orderBy("startYear")
result_task_2.write.csv("/app/data/result_task_2.csv", header=True, mode="overwrite") 


tv_series_votes = title_episode.join(title_ratings, "tconst").groupBy("parentTconst", "seasonNumber").agg(F.avg("numVotes").alias("avgVotes"))

result_task_3 = tv_series_votes.groupBy("seasonNumber").agg(F.avg("avgVotes").alias("avgVotes")).orderBy("seasonNumber")
result_task_3.write.csv("/app/data/result_task_3.csv", header=True, mode="overwrite") 


recent_movies = title_basics.filter(F.col("startYear") >= 1990)

total_runtime_by_genre = recent_movies.groupBy("genres").agg(F.sum("runtimeMinutes").alias("totalRuntime"))

windowSpecGenre = Window.orderBy(F.desc("totalRuntime"))
ranked_genres = total_runtime_by_genre.withColumn("rank", F.rank().over(windowSpecGenre))

result_task_4 = ranked_genres.select("genres", "totalRuntime", "rank")
result_task_4.write.csv("/app/data/result_task_4.csv", header=True, mode="overwrite") 



director_movies = title_crew.join(title_basics, "tconst")

average_gap_by_director = director_movies.groupBy("directors").agg(F.avg("startYear").alias("avgStartYear"))

std_dev_by_director = director_movies.groupBy("directors").agg(F.stddev_pop("startYear").alias("stdDevStartYear"))

result_task_5 = average_gap_by_director.join(std_dev_by_director, "directors")

result_task_5.write.csv("/app/data/result_task_5.csv", header=True, mode="overwrite")


movies_with_decades = title_basics.filter(col("titleType") == "movie") \
    .withColumn("decade", floor(col("startYear") / 10) * 10)

runtime_distribution_by_decade = movies_with_decades.groupBy("decade") \
    .agg(expr("percentile_approx(runtimeMinutes, 0.5)").alias("medianRuntime"),
         expr("percentile_approx(runtimeMinutes, 0.25)").alias("q1Runtime"),
         expr("percentile_approx(runtimeMinutes, 0.75)").alias("q3Runtime"))

runtime_distribution_by_decade.write.csv("/app/data/result_task_6.csv", header=True, mode="overwrite")

