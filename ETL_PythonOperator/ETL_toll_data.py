from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import tarfile
import csv

default_args = {
'owner' : 'hojun',
'start_date' : datetime.today(),
'email' : ['email@email.com'],
'retries' : 1, 
'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
dag_id = 'ETL_toll_data',
schedule_interval = timedelta(days = 1),
default_args = default_args,
description = 'Apache Airflow Final Assignment'
)

def download_dataset():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    destination = "/home/project/airflow/dags/python_etl/staging"
  
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(destination, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)

    print(f"File downloaded successfully to {destination}")

def untar_dataset():
    source = "/home/project/airflow/dags/python_etl/staging/tolldata.tgz"
    destination = "/home/project/airflow/dags/python_etl/staging"

    with tarfile.open(source, "r:gz") as tar:
        tar.extractall(path=destination)

    print(f"File extracted to {destination}")

def extract_data_from_csv():
    source = "/home/project/airflow/dags/python_etl/staging/vehicle-data.csv"
    destination = "/home/project/airflow/dags/python_etl/staging/csv_data.csv"

    with open(source, 'r') as infile, open(destination, 'w', newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow([row[0], row[1], row[2], row[3]])

    print(f"CSV data extracted to {destination}")

def extract_data_from_tsv():
    source = "/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv"
    destination = "/home/project/airflow/dags/python_etl/staging/tsv_data.csv"

    with open(source, 'r') as infile, open(destination, 'w', newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow([row[4], row[5], row[6]])

    print(f"TSV data extracted to {destination}")

def extract_data_from_fixed_width():
    source = "/home/project/airflow/dags/python_etl/staging/payment-data.txt"
    destination = "/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv"

    with open(source, 'r') as infile, open(destination, 'w', newline="") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            writer.writerow([row[5], row[6]])

    print(f"TXT data extracted to {destination}")

def consolidate_data():
    basepath = "/home/project/airflow/dags/python_etl/staging/"

    csvfile = basepath + 'csv_data.csv'
    tsvfile = basepath + 'tsv_data.csv'
    fixed_file = basepath + 'fixed_width_data.csv'
    outputfile = basepath + 'extracted_data.csv'

    with open(csvfile, 'r') as f1, open(tsvfile, 'r') as f2, \
        open(fixed_file, 'r') as f3, open(outputfile, 'w', newline="") as outfile:

        reader1 = csv.reader(f1)
        reader2 = csv.reader(f2)
        reader3 = csv.reader(f3)
        writer = csv.writer(outfile)

        for row1, row1, row3 in zip(reader1, reader2, reader3):
            combined_row = row1 + row2 + row3
            writer.writerow(combined_row)

    print(f"Consolidated data saved to {outputfile}")

def transform_data():
    basepath = "/home/project/airflow/dags/python_etl/staging/"
    source = basepath + 'extracted_data.csv'
    destination = basepath + 'transformed_file'

    with open(source, 'r') as infile, \
         open(destination, 'w', newline="") as outfile:
        reader = csv.reader(source)
        writer = csv.writer(destination)

        for row in reader:
            row[8] = row[8].upper()
            writer.writerow(row)

    print(f"Transformed data saved to {destination}")

download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag
)

untar_dataset = PythonOperator(
    task_id = 'untar_dataset',
    python_callable = untar_dataset,
    dag=dag
)

extract_data_from_csv = PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable = extract_data_from_csv,
    dag=dag
)

extract_data_from_tsv = PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable = extract_data_from_tsv, 
    dag=dag
)

extract_data_from_fixed_width = PythonOperator(
    task_id = 'extract_data_from_fixed_width',
    python_callable = extract_data_from_fixed_width,
    dag=dag
)

consolidate_data = PythonOperator(
    task_id = 'consolidate_data',
    python_callable = consolidate_data,
    dag=dag
)

transform_data = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform_data,
    dag=dag
)

download_dataset >> untar_dataset >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data




