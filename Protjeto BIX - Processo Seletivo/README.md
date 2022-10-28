<h1> Projeto BIX - Processo Seletivo </h1>

Hello guys

This project I made for the process of a company called BIX in which I had to create a data pipeline, from extracting data from sources such as api, postgresql database and parquet file, going through the treatment, to making it available in the database. data for the user.

For this to be possible, in the first step I had to enable the airflow orchestrator in my place using docker.

In the data pipeline, I started extracting data from postgre. For that I made the connection with the external bank, made the extraction, processed the data, made the connection with my local bank, created the table and inserted the data.

In the second part, I requested the data for the available api, processed the data, connected with the local bank, created another table and inserted the data.

Finishing the extraction, I took the data from the parquet file, processed the data, executed the connection with the local database, added the table and finally the insertion.

At the end of the code I created the DAG to run on airflow.

If you have any questions, do not hesitate to ask. Hug!
