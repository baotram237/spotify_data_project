## Airflow DAGs Overview

This project has three DAGs connected with dataset dependencies:

- **Album DAG** → Extract new release albums, publish `albums`, loads to S3 + Redshift, runs dbt models.
- **Tracks DAG** → Triggered by `albums`, extracts track details, loads to S3 + Redshift, runs dbt models.
- **Category DAG** → Extracts categories, loads to S3 + Redshift, runs dbt models.
![Dags](images/Dag_illustrations.png)

1. Dags examples:
![Categories dag](https://github.com/baotram237/spotify_data_project/blob/main/images/categories_dag.png)

![Tracks dag](images/https://github.com/baotram237/spotify_data_project/blob/main/images/tracks_dag.png)

2. Data in Redshift:
![sample data in Redshift](images/https://github.com/baotram237/spotify_data_project/blob/main/images/bronze_categories_exp.png)


