import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    'etl_car_accident_data',
    default_args=default_args,
    schedule_interval=None  
) as dag:

    @task(multiple_outputs=True)
    def download_data(filepath='/opt/airflow/data/monroe-county-crash.csv'):
        try:

            df = pd.read_csv(filepath, encoding='ISO-8859-1') 
        except UnicodeDecodeError as e:
            print(f"Error reading the file with encoding 'ISO-8859-1': {e}")
            df = pd.read_csv(filepath, encoding='utf-16')  

     
        print("First few rows of the data:", df.head())
        return {'data': df.to_dict(orient='records')}

    @task(multiple_outputs=True)
    def count_accidents_per_year(data: dict) -> dict:
        """
        Counts the number of accidents per year from the provided data.
        Assumes 'year' is a column in the data.
        """
    
        df = pd.DataFrame(data['data'])
        df.columns = df.columns.str.strip().str.lower()
        print("Columns in the DataFrame:", df.columns)
        print("First few rows of the DataFrame:", df.head())
        if 'year' not in df.columns:
            raise KeyError("'year' column not found in the data")

        accidents_per_year = df.groupby('year').size().to_dict()
        accidents_per_year = {str(year): count for year, count in accidents_per_year.items()} 

        return accidents_per_year


    @task
    def print_results(accidents_per_year: dict) -> None:
        """
        Prints the count of accidents per year to the console.
        """
        print("Accidents per year:")
        for year, count in accidents_per_year.items():
            print(f"Year: {year}, Accidents: {count}")

    df_data = download_data(filepath='/opt/airflow/data/monroe-county-crash.csv') 
    accidents_per_year = count_accidents_per_year(df_data)  
    print_results(accidents_per_year)
