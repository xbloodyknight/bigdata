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

from pyspark.sql.functions import col, count, explode, countDistinct

if 1 in n_query:
    episode_df = title_episode.alias("episode")
    crew_df = title_crew.alias("crew")
    basics_df = title_basics.alias("basics")

    # Filter TV episodes since 2010 and join with crew and basics dataframes
    prolific_writers = (
        episode_df.join(crew_df, col("episode.tconst") == col("crew.tconst"))
        .join(basics_df, col("episode.tconst") == col("basics.tconst"))
        .filter((col("basics.titleType") == "tvEpisode") & (col("basics.startYear") >= 2010))
        .select(explode(col("crew.writers")).alias("writer"), col("episode.tconst"))
        .groupBy("writer")
        .agg(count("episode.tconst").alias("num_episodes"))
        .orderBy(col("num_episodes").desc())
        .limit(10)
    )
    prolific_writers.show()

if 2 in n_query:
    # Calculate average number of episodes per season for series with more than 5 seasons
    avg_episodes_per_season = (
        title_episode.groupBy("parentTconst")
        .agg(count("episodeNumber").alias("total_episodes"),
            countDistinct("seasonNumber").alias("total_seasons"))
        .filter(col("total_seasons") > 5)
        .select(col("parentTconst"), (col("total_episodes") / col("total_seasons")).alias("avg_episodes"))
    )
    avg_episodes_per_season.show()
    
if 3 in n_query:
    from pyspark.sql.functions import col, count, array_contains

    # Create aliases for DataFrames to avoid ambiguity
    principals_df = title_principals.alias("principals")
    basics_df = title_basics.alias("basics")
    name_df = name_basics.alias("name")

    # Identify top 3 actors with the most leading roles in 'Drama' genre
    top_actors_in_drama = (
        principals_df.join(basics_df, col("principals.tconst") == col("basics.tconst"))
        .join(name_df, col("principals.nconst") == col("name.nconst"))
        .filter(array_contains(col("basics.genres"), "Drama") & col("principals.category").isin(["actor", "actress"]))
        .groupBy(col("name.primaryName"))
        .count()
        .orderBy(col("count").desc())
        .limit(3)
    )
    top_actors_in_drama.show()


if 4 in n_query:
    from pyspark.sql.functions import when, sum as sum_

    # Calculate the increase in movie production in each genre since 2000
    increase_in_production = (
        title_basics.filter(col("titleType") == "movie")
        .select(explode(col("genres")).alias("genre"), col("startYear"))
        .groupBy("genre")
        .agg(sum_(when(col("startYear") < 2000, 1).otherwise(0)).alias("before_2000"),
            sum_(when(col("startYear") >= 2000, 1).otherwise(0)).alias("after_2000"))
        .select(col("genre"), (col("after_2000") - col("before_2000")).alias("increase"))
        .orderBy(col("increase").desc())
    )
    increase_in_production.show()

if 5 in n_query:
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, col
    # Creating aliases for DataFrames
    principals_df = title_principals.alias("principals")
    basics_df = title_basics.alias("basics")

    # Creating a window specification for calculating year-over-year change
    year_over_year_window = Window.partitionBy("principals.nconst").orderBy("basics.startYear")

    # Join title_principals with title_basics with disambiguated column references
    actors_growth_in_films = (
        principals_df.join(basics_df, principals_df.tconst == basics_df.tconst)
        .filter((col("basics.titleType") == "movie") & (col("principals.category").isin(["actor", "actress"])))
        .groupBy("principals.nconst", col("basics.startYear"))
        .agg(count("principals.tconst").alias("films_count"))
        .withColumn("previous_year_count", lag(col("films_count")).over(year_over_year_window))
        .withColumn("growth", col("films_count") - col("previous_year_count"))
        .orderBy(col("growth").desc(), "principals.nconst")
    )

    actors_growth_in_films.show()



if 6 in n_query:
    # Create aliases for DataFrames to avoid ambiguity
    principals_df = title_principals.alias("principals")
    basics_df = title_basics.alias("basics")
    name_df = name_basics.alias("name")

    # Actors with the most diverse genres in their filmography
    diverse_genre_actors = (
        principals_df.join(basics_df, col("principals.tconst") == col("basics.tconst"))
        .join(name_df, col("principals.nconst") == col("name.nconst"))
        .filter(col("principals.category").isin(["actor", "actress"]))
        .select(col("name.primaryName"), col("basics.genres"))
        .withColumn("genre", explode(col("genres")))
        .groupBy("primaryName")
        .agg(countDistinct("genre").alias("distinct_genres"))
        .orderBy(col("distinct_genres").desc())
    )
    diverse_genre_actors.show()

