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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, count, rank, max
from pyspark.sql.window import Window


# Concat string arrays
title_akas = title_akas.withColumn("types", concat_ws(",", title_akas["types"]))
title_akas = title_akas.withColumn("attributes", concat_ws(",", title_akas["attributes"]))
name_basics = name_basics.withColumn("primaryProfession", concat_ws(",", name_basics["primaryProfession"]))
name_basics = name_basics.withColumn("knownForTitles", concat_ws(",", name_basics["knownForTitles"]))
title_crew = title_crew.withColumn("directors", concat_ws(",", title_crew["directors"]))
title_crew = title_crew.withColumn("writers", concat_ws(",", title_crew["writers"]))
title_basics = title_basics.withColumn("genres", concat_ws(",", title_basics["genres"]))


# Business Question 1: Number of movies produced each year and average runtime
movies_by_year = title_basics.filter((col("titleType") == "movie") & 
                                     title_basics["startYear"].isNotNull() & 
                                     title_basics["runtimeMinutes"].isNotNull()) \
    .groupBy("startYear") \
    .agg(count("*").alias("numMovies"), avg("runtimeMinutes").alias("averageRuntime")) \
    .orderBy("startYear")

movies_by_year.show(truncate=False)
movies_by_year.write.csv("/app/data/result_question_1.csv", header=True, mode="overwrite")


# Business Question 2: Top 10 movie genres based on the average ratings
filtered_movies = title_basics.filter(col("titleType") == "movie")
genre_ratings = filtered_movies.join(title_ratings, filtered_movies["tconst"] == title_ratings["tconst"]) \
    .groupBy("genres") \
    .agg(avg("averageRating").alias("avgRating")) \
    .orderBy(desc("avgRating")) \
    .limit(10)

genre_ratings.show(truncate=False)
genre_ratings.write.csv("/app/data/result_question_2.csv", header=True, mode="overwrite")


# Business Question 3: Distribution of average ratings for movies in different languages
language_ratings = title_akas.join(
    title_ratings,
    title_akas["titleId"] == title_ratings["tconst"]
).join(
    title_basics,
    title_akas["titleId"] == title_basics["tconst"]
).filter(
    col("titleType") == "movie"
).groupBy(
    title_akas["language"]
).agg(
    avg(title_ratings["averageRating"]).alias("avgRating")
)

language_ratings.show(truncate=False)
language_ratings.write.csv("/app/data/result_question_3.csv", header=True, mode="overwrite")


# Business Question 4: 10 youngest actors and actresses who starred in more than 5 films
youngest_actors = name_basics.join(title_principals.alias("tp"), name_basics["nconst"] == col("tp.nconst")) \
    .join(title_basics.alias("tb"), col("tp.tconst") == col("tb.tconst")) \
    .filter((col("birthYear").isNotNull()) & (col("category").isin("actor", "actress")) & (col("titleType") == "movie")) \
    .groupBy(name_basics["primaryName"], name_basics["birthYear"], col("tp.category")) \
    .agg(count("tp.tconst").alias("film_count")) \
    .filter(col("film_count") > 5) \
    .orderBy(name_basics["birthYear"].desc()) \
    .limit(10)

youngest_actors.show(truncate=False)
youngest_actors.write.csv("/app/data/result_question_4.csv", header=True, mode="overwrite")


# Business Question 5: Top 5 actors/actresses with the most collaborations
collaborations = (
    title_principals.alias("principals1")
    .join(name_basics.alias("names1"), col("principals1.nconst") == col("names1.nconst"))
    .join(title_principals.alias("principals2"), col("principals1.tconst") == col("principals2.tconst"))
    .join(name_basics.alias("names2"), col("principals2.nconst") == col("names2.nconst"))
    .filter((col("principals1.category") == "actor") | (col("principals1.category") == "actress"))
    .filter((col("principals2.category") == "actor") | (col("principals2.category") == "actress"))
    .filter(col("principals1.nconst") < col("principals2.nconst"))
    .groupBy(col("names1.primaryName").alias("Actor1"), col("names2.primaryName").alias("Actor2"))
    .agg(count("*").alias("numCollaborations"))
    .orderBy(desc("numCollaborations"))
    .limit(5)
)

collaborations.show(truncate=False)
collaborations.write.csv("/app/data/result_question_5.csv", header=True, mode="overwrite")


# Business Question 6: Longest-running TV series that lasts more than 50 years
longest_running_tv_series = title_basics.filter((col("titleType") == "tvSeries") & 
                                                (col("startYear").isNotNull()) & 
                                                (col("endYear").isNotNull()) & 
                                                (col("endYear") - col("startYear") > 50)) \
    .select("primaryTitle", "startYear", "endYear", (col("endYear") - col("startYear")).alias("duration")) \
    .orderBy("duration", ascending=False)

longest_running_tv_series.show(truncate=False)
longest_running_tv_series.write.csv("/app/data/result_question_6.csv", header=True, mode="overwrite")