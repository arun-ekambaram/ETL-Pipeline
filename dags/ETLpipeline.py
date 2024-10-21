from airflow import DAG
# SimpleHttpOperator is basically linked with the task where when we will be calling with the APIs 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
# PostgresHook will be linked with that particular task, wherein we will be pushing the data into the PostgreSQL.
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

#Define the DAG

with DAG(
    dag_id ='nasa_apod_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catcup = False
) as dag:
    
    ## step 1: Create the table if it doesn't exist

    @task
    def create_table():
        ## initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        ## SQL query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
            );
        

        """
        ##Execute SQL query
        postgres.hook.run(create_table_query)


    ## step 2: Extract the NASA API Data(APOD) - Astronomy Picture of the day [Extract Pipleline]
    # https://api.nasa.gov/planetary/apod?api_key=HdXTz8iHZ7HGKYiyXazXkrwKnPOHQnhNCd1Tj3r7
    extract_apod=SimpleHttpOperator(
        task_id = 'extract_apod',
        http_conn_id = 'nasa_api', ## Connection ID defined in Airflow for NASA API
        endpoint = 'planetary/apod', ## NASA API endpoint for Astronomy Picture of the day
        method= 'GET',
        data= {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, ## using the API key from the connection 
        response_filter= lambda response: response.json(), ## Convert response to json
    ) 
  
    ## step 3: Transform the data(Pick the information that i need to save)

    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title',''),
            'explanation': response.get('explanation',''),
            'url': response.get('url',''),
            'date': response.get('date',''),
            'media_type': response.get('media_type','')  
        }
        return apod_data
    
    ## step 4: Load the data into PostgresSQL
    @task
    def load_data_to_postgres(apod_data):
        ## initialize the postgreshook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        ## Define the sql insert query

        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute SQL query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    ## step 5: Verify the data DBViewer (will help you to connect to any database)

    ## step 6: Define the task dependencies
    ## Extract

    create_table() >> extract_apod ## Ensure the table is created before extraction
    api_response = extract_apod.output

    ## Transform

    transformed_data=  transform_apod_data(api_response)

    ## Load
    load_data_to_postgres(transformed_data)