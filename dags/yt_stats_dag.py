from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash_operator import BashOperator

create_dataset_command = """
        bq mk --dataset \
            --project_id=airflow-yt-stats \
            --description='Youtube Global Statistics 2023' \
            airflow-yt-stats:yt_stats
        """

default_args = {
    'start_date': datetime(2023, 9, 21),
    'schedule_interval': None,
    'catchup': False,
    'tags': ['yt_global_stats'],
}


with DAG(
    'global_youtube_stats',
    default_args=default_args,
    description='Retail DAG',
    catchup=False,
) as dag:

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/global_yt_statistics.csv',
        dst='raw/global_yt_statistics.csv',
        bucket='cdt_yt_global_stats',
        gcp_conn_id='gcp',
        mime_type='text/csv',
        dag=dag,
    )

    create_yt_stats_dataset = BashOperator(
        task_id='create_yt_stats_dataset',
        bash_command=create_dataset_command,
        dag=dag,
    )

    upload_csv_to_gcs >> create_yt_stats_dataset
