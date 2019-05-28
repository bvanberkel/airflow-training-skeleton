import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

args = {'owner': 'bas', 'start_date': airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
)


weekday_person_to_email = {
    0: "Bob",
    1: "Joe",
    2: "Alice",
    3: "Joe",
    4: "Alice",
    5: "Alice",
    6: "Alice"}


def get_person_to_email(execution_date, **context):
    return weekday_person_to_email[execution_date.weekday()]


branching = BranchPythonOperator(task_id="branching",
                                 python_callable=get_person_to_email,
                                 provide_context=True, dag=dag)


print_execution_date = BashOperator(
    task_id="print_execution_date", bash_command="echo {{ execution_date }}", dag=dag
)


final_task = DummyOperator(task_id='final_task', dag=dag, trigger_rule='one_success')


print_execution_date >> branching

for person in ['Alice', 'Bob', 'Joe']:
    branching >> DummyOperator(task_id='email_' + person, dag=dag) >> final_task
