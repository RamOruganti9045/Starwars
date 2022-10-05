
The Project is to download the Star-wars data from swapi.dev and to find the oldest character ineach film .

The project will download People and film data from swapi.dev .

The technologies used :
	Airflow and Postgres SQL

How to run :

Included docker-compose file, so you can up spin up the airflow using docker with docker-compose file
Connect postgres server to airflow using connection id : postgres_local

Once done imply trigger the Ingestion dag which will ingest the data and in turn will also trigger the aggregate data Dag also if Ingestion DAG is successfull.

