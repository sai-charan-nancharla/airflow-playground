from airflow.providers.standard.operators.bash import BashOperator;
from airflow.sdk import dag, task;

@dag(
    dag_id="operators_dag"
)
def operators_dag():

    @task.python
    def python_task():
        print('Python task with taskAPI operator');

    @task
    def python_task_simple():
        print('Python task with simple task decorator');

    @task.bash
    def new_bash_task():
        return "echo 'Hello from Airflow 3.0 Bash task!'"

    old_bash = BashOperator(
        task_id='old_bash_task',
        bash_command="echo 'Hello from Airflow 2.0 BashOperator!'"
    )

    # Defining task dependencies    
    #Old Operators are not functions they can be called with variable name and can be used in task dependencies
    python_task() >> python_task_simple() >> new_bash_task() >> old_bash
    # python_task() >> python_task_simple() >> new_bash_task()


operators_dag()