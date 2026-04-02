from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
'owner' : 'hojun',
'start_date' : datetime.today(),
'email' : ['email@email.com'],
'email_on_failure' : True,
'email_on_retry' : True,
'retries' : 1,
'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
dag_id='ETL_toll_data',
schedule_interval = timedelta(days=1),
default_args = default_args,
description = 'Apache Airflow Final Assignment'
)

unzip_data = BashOperator(
task_id = 'unzip_data',
bash_command = 'tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz',
dag=dag
)

extract_data_from_csv = BashOperator(
task_id = 'extract_data_from_csv',
bash_command = "cut -d',' -f1,2,3,4 vehicle-data.csv > csv_data.csv",
dag=dag
)

extract_data_from_tsv = BashOperator(
task_id = 'extract_data_from_tsv',
bash_command = "cut -d$'\t' -f5,6,7 tollplaza-data.tsv > tsv_data.csv",
dag=dag
)

extract_data_from_fixed_width = BashOperator(
task_id = "extract_data_from_fixed_width",
bash_command = "cut -c59-61,63-67 payment-data.txt > fixed_width_data.csv",
dag=dag
)

consolidate_data = BashOperator(
task_id = 'consolidate_data',
bash_command = "paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv",
dag=dag
)

transform_data = BashOperator(
task_id = 'transform_data',
bash_command="""
cut -d',' -f1-3 extracted_data.csv > tmp1.csv
cut -d',' -f4 extracted_data.csv | tr 'a-z' 'A-Z' > tmp2.csv
cut -d',' -f5-9 extracted_data.csv > tmp3.csv
paste -d',' tmp1.csv tmp2.csv tmp3.csv > /home/project/airflow/dags/finalassignment>
""",
dag=dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidat>


