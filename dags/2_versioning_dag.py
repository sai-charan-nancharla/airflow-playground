from airflow.sdk import dag, task;


@dag(
        dag_id="versioning_dag"
)
# Airflow will create new version of the DAG if any changes are made to the code. This is useful for tracking changes and maintaining a history of the DAG's evolution over time.
# We can see latest version of the DAG in Airflow UI. 
# If we want to see older versions of the DAG, on Graph view and click settings and able to see & change the dag versions..
def versioning_dag(): 
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
    
    @task.python
    def fifth_task():
        print('Versioning task');

    # Defining task dependencies
    first = first_task()
    second = seccond_task()
    third = third_task()
    fourth = fourth_task()

    # Runs after sucessful completion of one after another [3,4] will run in parallel after 2nd task is completed
    first >> second >> [third, fourth] >> fifth_task()


# Registering / instantiating the DAG
versioning_dag()