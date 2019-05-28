import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {'owner': 'bas', 'start_date': airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise2",
    default_args=args,
)

print_execution_date = BashOperator(
    task_id="print_execution_date", bash_command="echo {{ execution_date }}", dag=dag
)

wait_5 = BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

wait_1 = BashOperator(
    task_id="wait_5", bash_command="sleep 1", dag=dag
)

wait_10 = BashOperator(
    task_id="wait_5", bash_command="sleep 10", dag=dag
)

the_end = BashOperator(
    task_id="the_end", bash_command="echo the end", dag=dag
)

print_execution_date >> wait_1
print_execution_date >> wait_5
print_execution_date >> wait_10
wait_1 >> the_end
wait_5 >> the_end
wait_10 >> the_end