from pyspark.sql import functions as F
from pyspark.sql.window import Window
from extract import (
    title_basics, 
    title_akas, 
    name_basics, 
    title_crew,
    title_episode, 
    title_principals,
    title_ratings
)

n_query = [1,2,3,4,5,6]



if 1 in n_query:

    movies = title_basics.join(title_ratings, on="tconst").filter(
        (title_basics.titleType == "movie") &
        (title_basics.startYear > 2010) &
        (title_ratings.numVotes >= 1000)
    )

    
    movies_with_crew = movies.join(title_crew, on="tconst")

    
    movies_with_crew = movies_with_crew.withColumn("director", F.explode("directors"))

    
    avg_rating_directors = movies_with_crew.groupBy("director").agg(F.avg("averageRating").alias("avgRating"))

    
    window_spec = Window.orderBy(F.desc("avgRating"))
    ranked_directors = avg_rating_directors.withColumn("rank", F.rank().over(window_spec))

    
    directors_with_names = ranked_directors.join(name_basics, ranked_directors.director == name_basics.nconst)

    
    top_10_directors = directors_with_names.select("primaryName", "avgRating").orderBy(F.desc("avgRating")).limit(10)
    top_10_directors.show(truncate=False)


if 2 in n_query:
    
    series_with_episodes = title_basics.join(title_episode, title_basics.tconst == title_episode.parentTconst) \
        .filter(
            (title_basics.titleType == "tvSeries")
            & (F.col("episodeNumber").cast("int") > 50)
        ).select(title_basics["*"], title_episode["episodeNumber"])

    
    median_runtime_per_genre = series_with_episodes.withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int")) \
        .groupBy("genres") \
        .agg(F.expr('percentile_approx(runtimeMinutes, 0.5)').alias("medianRuntime"))

    
    window_spec = Window.orderBy(F.desc("medianRuntime"))
    ranked_genres = median_runtime_per_genre.withColumn(
        "rank", F.rank().over(window_spec)
    )

    
    top_5_genres = ranked_genres.filter(F.col("medianRuntime").isNotNull()).filter(F.col("rank") <= 5)
    top_5_genres.show(n=5, truncate=False)
    bottom_5_genres = ranked_genres.filter(F.col("medianRuntime").isNotNull()).orderBy("medianRuntime").limit(5)
    bottom_5_genres.show(n=5, truncate=False)
    

if 3 in n_query:

    writer_director_pairs = title_crew.withColumn("director", F.explode("directors")) \
                                    .withColumn("writer", F.explode("writers")) \
                                    .filter((F.col("director") != "\\N") & (F.col("writer") != "\\N")) \
                                    .groupBy("director", "writer") \
                                    .count() \
                                    .orderBy(F.desc("count"))

    
    directors_with_names = writer_director_pairs.join(name_basics, writer_director_pairs.director == name_basics.nconst) \
                                                .select("director", "primaryName", "writer", "count") \
                                                .withColumnRenamed("primaryName", "directorName")

    
    writers_with_names = directors_with_names.join(name_basics, directors_with_names.writer == name_basics.nconst) \
                                            .select("directorName", "primaryName", "count") \
                                            .withColumnRenamed("primaryName", "writerName")

    
    writers_with_names.orderBy(F.desc("count")).show(5, truncate=False)



if 4 in n_query:
    
    actors_ratings = title_principals.join(title_ratings, "tconst") \
                                    .join(title_basics, "tconst") \
                                    .filter(title_principals["category"] == "actor") \
                                    .select("nconst", "averageRating", "startYear")

    
    windowSpec = Window.partitionBy("nconst").orderBy("startYear")

    
    actors_increase = actors_ratings.withColumn("prevRating", F.lag("averageRating").over(windowSpec)) \
                                    .withColumn("ratingIncrease", F.col("averageRating") - F.col("prevRating")) \
                                    .filter(F.col("ratingIncrease").isNotNull()) \
                                    .orderBy(F.desc("ratingIncrease"))

    
    actors_with_names = actors_increase.join(name_basics, actors_increase.nconst == name_basics.nconst) \
                                    .select("primaryName", "ratingIncrease") \
                                    .orderBy(F.desc("ratingIncrease"))

    actors_with_names.show()

if 5 in n_query:
    
    
    animated_titles_by_country = title_akas.join(title_basics, title_akas["titleId"] == title_basics["tconst"]) \
                                            .filter((title_basics["titleType"] == "movie") & 
                                                    (F.array_contains(title_basics["genres"], "Animation")) & 
                                                    (title_basics["startYear"] >= 2000)) \
                                            .groupBy("region") \
                                            .count() \
                                            .orderBy(F.desc("count"))

    animated_titles_by_country.show()



if 6 in n_query:
    
    
    actor_genre_longevity = title_principals.join(title_basics, "tconst") \
                                            .filter(title_principals["category"] == "actor") \
                                            .withColumn("genre", F.explode("genres")) \
                                            .groupBy("nconst", "genre") \
                                            .agg(F.min("startYear").alias("startYear"), F.max("endYear").alias("endYear")) \
                                            .withColumn("careerLength", F.col("endYear") - F.col("startYear"))

    
    actors_with_names = actor_genre_longevity.join(name_basics, actor_genre_longevity.nconst == name_basics.nconst) \
                                            .select("primaryName", "genre", "careerLength") \
                                            .orderBy(F.desc("careerLength"))

    actors_with_names.show()

