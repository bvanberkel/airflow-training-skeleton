import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.http_hook import HttpHook


args = {"owner": "bas",
        "start_date": airflow.utils.dates.days_ago(3)}

dag = DAG(dag_id="exercise4",
          default_args=args,
          schedule_interval="0 0 * * *")


class HTTPToCloudStorageOperator(BaseOperator):

    template_fields = ('endpoint', 'data', 'destination_cloud_storage_uris', 'labels')
    template_ext = ('.sql',)
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
                 http_conn_id='http_default',
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
        hook.upload(bucket=self.bucket, object=response, filename=self.filename, mime_type='application/json')


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(task_id="postgres_to_gcs",
                                                   sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
                                                   bucket="bvb-data",
                                                   filename="daily_load_{{ ds }}",
                                                   postgres_conn_id="post-conn",
                                                   dag=dag)

http_to_gsc = HTTPToCloudStorageOperator(task_id="exchange_rate_to_gcs",
                                         endpoint='https://europe-west2-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date={{ ds }}&to=EUR',
                                         bucket="bvb-data",
                                         filename="exchange_rate_{{ ds }}",
                                         postgres_conn_id="post-conn",
                                         dag=dag)

pgsl_to_gcs
http_to_gsc