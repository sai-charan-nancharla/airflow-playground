from airflow.sdk import dag, task;


@dag(
        dag_id="first_dag_name"
)
# takes fun name as dag name if not explictly mentioned like above
def first_dag(): 
    @task.python
    def first_task():
        print('First task');

    @task.python
    def seccond_task():
        print('Second task');

    @task.python
    def third_task():
        print('Third task');

    @task.python
    def fourth_task():
        print('Fourth task');

    # Defining task dependencies
    first = first_task()
    second = seccond_task()
    third = third_task()

    # Runs after sucessful completion of one after another
    first >> second >> third 


# Registering / instantiating the DAG
first_dag()