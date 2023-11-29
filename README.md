# bigdata

## Dataset preparation
1. Create `dataset` subdirectory
2. [Download](https://datasets.imdbws.com/) all 7 files to it
3. Unzip all files
4. Ensure following files exist
`name_basics.csv/data.tsv`
`title_akas.csv/data.tsv`
`title_basics.csv/data.tsv`
`title_crew.csv/data.tsv`
`title_episode.csv/data.tsv`
`title_principals.csv/data.tsv`
`title_ratings.csv/data.tsv`

## Running
1. Install Docker
2. Clone repository
3. Open terminal in repository
4. Run **(takes long time)** `docker build -t my-spark-img .`
5. Run **(takes _even longer_ time)** `docker run -v  ${PWD}\data:/app/data my-spark-img `
6. Write queries for your business questions in `query.py` 
