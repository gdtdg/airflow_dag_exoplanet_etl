# Airflow dag for exoplanet etl

This is an Airflow dag linked with two projects:

- [the Nasa Exoplanet Website](https://github.com/gdtdg/Nasa_exoplanet_website),
- [the Nasa Exoplanet Website React](https://github.com/gdtdg/project_nasa_exoplanet_react)

### Tech Stack:


![ElasticSearch](https://img.shields.io/badge/-ElasticSearch-005571?style=for-the-badge&logo=elasticsearch)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)

### How it works:

You have a TransformService class that takes one input repository and one output repository, both are instances of the Reposity class.

For this project the input repository takes his data from an api, and the data in the output repository are push in an elasticsearch index.

As long as your repositories respects the Repository class, you can create any instance of Repository with any data or push in any database you wish.



