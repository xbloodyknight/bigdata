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

# Select first 20
title_basics = title_basics.limit(20) 
title_akas = title_akas.limit(20) 
name_basics = name_basics.limit(20) 
title_crew = title_crew.limit(20)
title_episode = title_episode.limit(20) 
title_principals = title_principals.limit(20)
title_ratings = title_ratings.limit(20)

# Concat string arrays
title_akas = title_akas.withColumn("types", concat_ws(",", title_akas["types"]))
title_akas = title_akas.withColumn("attributes", concat_ws(",", title_akas["attributes"]))
name_basics = name_basics.withColumn("primaryProfession", concat_ws(",", name_basics["primaryProfession"]))
name_basics = name_basics.withColumn("knownForTitles", concat_ws(",", name_basics["knownForTitles"]))
title_crew = title_crew.withColumn("directors", concat_ws(",", title_crew["directors"]))
title_crew = title_crew.withColumn("writers", concat_ws(",", title_crew["writers"]))
title_basics = title_basics.withColumn("genres", concat_ws(",", title_basics["genres"]))

# Write csv
title_basics.write.csv("/app/data/title_basics.csv", header=True, mode="overwrite") 
title_akas.write.csv("/app/data/title_akas.csv", header=True, mode="overwrite") 
name_basics.write.csv("/app/data/name_basics.csv", header=True, mode="overwrite") 
title_crew.write.csv("/app/data/title_crew.csv", header=True, mode="overwrite")
title_episode.write.csv("/app/data/title_episode.csv", header=True, mode="overwrite") 
title_principals.write.csv("/app/data/title_principals.csv", header=True, mode="overwrite")
title_ratings.write.csv("/app/data/title_ratings.csv", header=True, mode="overwrite")
