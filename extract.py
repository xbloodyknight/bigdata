from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import split

# Initialize Spark Session
spark = SparkSession.builder.appName("IMDb Dataset").getOrCreate()

# Define the schemas based on the dataset details

# Schema for title.akas
schema_title_akas = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", BooleanType(), True)
])

# Schema for title.basics
schema_title_basics = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
])

# Schema for title.crew
schema_title_crew = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])

# Schema for title.episode
schema_title_episode = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("episodeNumber", IntegerType(), True)
])

# Schema for title.principals
schema_title_principals = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
])

# Schema for title.ratings
schema_title_ratings = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True)
])

# Schema for name.basics
schema_name_basics = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])




# Assuming the schemas have been defined as shown earlier (e.g., schema_title_akas, schema_title_basics, etc.)
title_akas = spark.read.format("csv") \
                          .option("sep", "\t") \
                          .option("header", "true") \
                          .schema(schema_title_akas) \
                          .load("dataset/title.akas.tsv/data.tsv")  # Replace with your file path
title_akas = title_akas.withColumn("types", split(title_akas["types"], ",\s*"))
title_akas = title_akas.withColumn("attributes", split(title_akas["attributes"], ",\s*"))

# Read title.basics
title_basics = spark.read.format("csv") \
                           .option("sep", "\t") \
                           .option("header", "true") \
                           .schema(schema_title_basics) \
                           .load("dataset/title.basics.tsv/data.tsv")  # Replace with your file path
title_basics = title_basics.withColumn("genres", split(title_basics["genres"], ",\s*"))

# Read title.crew
title_crew = spark.read.format("csv") \
                         .option("sep", "\t") \
                         .option("header", "true") \
                         .schema(schema_title_crew) \
                         .load("dataset/title.crew.tsv/data.tsv")  # Replace with your file path
title_crew = title_crew.withColumn("directors", split(title_crew["directors"], ",\s*"))
title_crew = title_crew.withColumn("writers", split(title_crew["writers"], ",\s*"))

# Read title.episode
title_episode = spark.read.format("csv") \
                            .option("sep", "\t") \
                            .option("header", "true") \
                            .schema(schema_title_episode) \
                            .load("dataset/title.episode.tsv/data.tsv")  # Replace with your file path

# Read title.principals
title_principals = spark.read.format("csv") \
                              .option("sep", "\t") \
                              .option("header", "true") \
                              .schema(schema_title_principals) \
                              .load("dataset/title.principals.tsv/data.tsv")  # Replace with your file path

# Read title.ratings
title_ratings = spark.read.format("csv") \
                            .option("sep", "\t") \
                            .option("header", "true") \
                            .schema(schema_title_ratings) \
                            .load("dataset/title.ratings.tsv/data.tsv")  # Replace with your file path

# Read name.basics
name_basics = spark.read.format("csv") \
                          .option("sep", "\t") \
                          .option("header", "true") \
                          .schema(schema_name_basics) \
                          .load("dataset/name.basics.tsv/data.tsv")  # Replace with your file path
name_basics = name_basics.withColumn("primaryProfession", split(name_basics["primaryProfession"], ",\s*"))
name_basics = name_basics.withColumn("knownForTitles", split(name_basics["knownForTitles"], ",\s*"))
