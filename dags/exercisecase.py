import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook
from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataProcPySparkOperator, DataprocClusterDeleteOperator)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

class HTTPToCloudStorageOperator(BaseOperator):

    template_fields = ('endpoint', 'filename')
    template_ext = ()
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 bucket,
                 filename,
                 google_cloud_storage_conn_id='google_cloud_default',
                 method='POST',
                 data=None,
                 headers=None,
                 extra_options=None,
                 http_conn_id='http_echte_default',
                 delegate_to=None,
                 *args, **kwargs):
        super(HTTPToCloudStorageOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.extra_options = extra_options or {}
        self.bucket = bucket
        self.filename = filename
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = http.run(self.endpoint,
                            self.data,
                            self.headers,
                            self.extra_options)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        local_filename = '/tmp/' + self.filename
        f = open(local_filename, "w")
        f.write(response.text)
        f.close()
        hook.upload(bucket=self.bucket, object=self.filename, filename=local_filename, mime_type='application/json')


default_args = {"owner": "bas", "start_date": airflow.utils.dates.days_ago(3)}

dag = DAG(dag_id="case",
          default_args=default_args,
          schedule_interval="0 0 * * *")

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(task_id="postgres_to_gcs",
                                                   sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
                                                   bucket="bvb-data",
                                                   filename="daily_load_{{ ds }}",
                                                   postgres_conn_id="post-conn",
                                                   dag=dag)

http_to_gcs = HTTPToCloudStorageOperator(task_id="exchange_rate_to_gcs",
                                         endpoint='airflow-training-transform-valutas?date={{ ds }}&to=EUR',
                                         bucket="bvb-data",
                                         filename="exchange_rate_{{ ds }}",
                                         dag=dag)

dataproc_create_cluster = DataprocClusterCreateOperator(task_id="dataproc_create",
                                                        cluster_name="analyse-pricing-{{ ds }}",
                                                        project_id='airflowbolcom-may2829-aaadbb22',
                                                        num_workers=2,
                                                        zone="europe-west4-a",
                                                        dag=dag)

compute_aggregates = DataProcPySparkOperator(task_id="dataproc_run",
                                             main="gs://europe-west1-training-airfl-4ecc4ae4-bucket/build_statistics.py",
                                             cluster_name="analyse-pricing-{{ ds }}",
                                             arguments=["gs://bvb-data/daily_load_{{ ds}}",
                                                        "gs://bvb-data/exchange_rate_{{ ds }}",
                                                        "gs://bvb-data/output_file_{{ ds }}"],
                                             dag=dag)

dataproc_delete_cluster = DataprocClusterDeleteOperator(task_id="dataproc_delete",
                                                        cluster_name="analyse-pricing-{{ ds }}",
                                                        project_id='airflowbolcom-may2829-aaadbb22',
                                                        dag=dag)

gcstobq = GoogleCloudStorageToBigQueryOperator(task_id="gcs_to_bq",
                                               bucket="bvb-data",
                                               source_objects=["output_file_{{ ds }}/part-*"],
                                               destination_project_dataset_table="airflowbolcom-may2829-aaadbb22:prices.land_registry_price${{ ds_nodash }}",
                                               source_format="PARQUET",
                                               write_disposition="WRITE_TRUNCATE",
                                               autodetect=True,
                                               dag=dag)



pgsl_to_gcs >> dataproc_create_cluster
http_to_gcs >> dataproc_create_cluster
dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
dataproc_delete_cluster >> gcstobq
