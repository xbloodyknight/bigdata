from extract import (
    title_basics, 
    title_akas, 
    name_basics, 
    title_crew,
    title_episode, 
    title_principals,
    title_ratings
)
from pyspark.sql.types import IntegerType


from pyspark.sql.window import Window
from pyspark.sql import functions as F








min_votes_threshold = 10000  
mean_rating = title_ratings.agg(F.avg("averageRating")).first()[0]


highest_rated_movies = title_basics.join(title_ratings, "tconst") \
    .filter((title_basics['titleType'] == 'movie') & (title_basics['startYear'] > 2000))


weighted_rating = (F.col("numVotes") / (F.col("numVotes") + min_votes_threshold) * F.col("averageRating")) + \
                  (min_votes_threshold / (F.col("numVotes") + min_votes_threshold) * mean_rating)


highest_rated_movies_with_weighted_rating = highest_rated_movies.withColumn("weightedRating", weighted_rating) \
    .filter(F.col("numVotes") >= min_votes_threshold) \
    .orderBy(F.desc("weightedRating")) \
    .select("primaryTitle", "startYear", "averageRating", "numVotes", "weightedRating") \
    .limit(5)

highest_rated_movies_with_weighted_rating.show()












top_directors = title_crew.join(title_basics, "tconst") \
    .join(title_ratings, "tconst") \
    .filter((title_basics['titleType'] == 'tvSeries') & (title_ratings['averageRating'] > 8)) \
    .withColumn("director", F.explode("directors")) \
    .groupBy("director") \
    .count() \
    .withColumnRenamed("count", "seriesCount")


windowSpec = Window.orderBy(F.desc("seriesCount"))


top_directors_with_rank = top_directors.withColumn("rank", F.rank().over(windowSpec))


top_directors_with_names = top_directors_with_rank.join(name_basics, top_directors_with_rank['director'] == name_basics['nconst']) \
    .orderBy("rank").select("primaryName", "seriesCount", "rank")

top_directors_with_names.show()


action_movies = title_basics.filter(F.array_contains(title_basics['genres'], 'Action'))


actors_in_action_movies = action_movies.join(title_principals, "tconst") \
    .filter(title_principals['category'].isin(["actor", "actress"])) \
    .groupBy("nconst") \
    .count() \
    .filter("count > 3") \
    .join(name_basics, "nconst") \
    .select("primaryName")

actors_in_action_movies.show()



movies_1990s = title_basics.filter((title_basics['startYear'] >= 1990) & (title_basics['startYear'] < 2000))


average_runtime = movies_1990s.withColumn("genre", F.explode("genres")) \
    .filter(F.col("genre") != '\\N') \
    .groupBy("genre") \
    .agg(F.avg("runtimeMinutes").alias("averageRuntime"))

average_runtime.show()


def get_decade(year):
    if year is None:
        return None  
    else:
        return (year // 10) * 10


get_decade_udf = F.udf(get_decade, IntegerType())


title_basics_with_decade = title_basics.withColumn("decade", get_decade_udf(title_basics["startYear"]))


popular_genres = title_basics_with_decade.filter(title_basics_with_decade['decade'] >= 1970) \
    .withColumn("genre", F.explode("genres")) \
    .groupBy("decade", "genre") \
    .count() \
    .withColumn("rank", F.rank().over(Window.partitionBy("decade").orderBy(F.desc("count")))) \
    .filter("rank <= 3")

popular_genres.show()



actor_titles = title_principals.join(title_basics, title_principals["tconst"] == title_basics["tconst"]) \
    .filter((title_principals["category"].isin(["actor", "actress"])) & (title_basics["startYear"] >= 2000) & (title_basics["titleType"].isin(["movie", "tvSeries"])))


prolific_actors = actor_titles.groupBy(title_principals["nconst"]) \
    .agg(F.countDistinct(title_basics["tconst"]).alias("uniqueTitleCount"))


prolific_actors_with_names = prolific_actors.join(name_basics, prolific_actors["nconst"] == name_basics["nconst"]) \
    .select(name_basics["primaryName"], "uniqueTitleCount")


prolific_actors_ordered = prolific_actors_with_names.orderBy(F.desc("uniqueTitleCount"))


prolific_actors_ordered.show()




highest_rated_movies_with_weighted_rating.write.csv("/app/data/highest_rated_movies_with_weighted_rating.csv", header=True, mode="overwrite") 
top_directors_with_names.write.csv("/app/data/top_directors_with_names.csv", header=True, mode="overwrite") 
actors_in_action_movies.write.csv("/app/data/actors_in_action_movies.csv", header=True, mode="overwrite") 
average_runtime.write.csv("/app/data/average_runtime.csv", header=True, mode="overwrite")
popular_genres.write.csv("/app/data/popular_genres.csv", header=True, mode="overwrite") 
prolific_actors_ordered.write.csv("/app/data/prolific_actors_ordered.csv", header=True, mode="overwrite")
