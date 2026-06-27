from airflow.sdk import dag, task;

@dag(
    dag_id="xcoms_kwargs_dag"
)

def xcoms_kwargs_dag():

    @task.python
    def extract_task(**kwargs):
        print('Fetching Data');
        fetched_data = {"fetch_data": [1, 2, 3, 4, 5]}

        # We are using the current task instance to push the output of the current task to XComs to register it with this task's id.
        task_instance = kwargs['ti']
        task_instance.xcom_push(key='fetched_data', value=fetched_data)

    @task.python
    def transform_task(**kwargs):
        print('Transforming Data');
        
        # We are using the current task instance to pull the output of the previous task from XComs using the task_id of the previous task and the key we used to push the data.
        # we can get values from multiple tasks by using their task_ids and key.
        # task_instance.xcom_pull(task_ids=['task1', 'task2', 'task3'], key='data') -> returns a list of values from the specified tasks with the specified key.
        task_instance = kwargs['ti']
        first_task_output = task_instance.xcom_pull(task_ids='extract_task', key='fetched_data')

        print(f"Received output from first task: {first_task_output}")
        transformed_data = {"transform_data": [x * 2 for x in first_task_output["fetch_data"]]}
        task_instance.xcom_push(key='transformed_data', value=transformed_data)

    @task.python    
    def load_task(**kwargs):
        print('Loading Data');
        task_instance = kwargs['ti']
        second_task_output = task_instance.xcom_pull(task_ids='transform_task', key='transformed_data')
        print(f"Received output from second task: {second_task_output}")
        return "Data loaded successfully"

    # Defining task dependencies
    # Since we're using XComs explicitly, we need to define the dependencies between tasks. 
    # The output of one task is pushed to XComs and then pulled by the next task.
    first = extract_task()
    second = transform_task()
    third = load_task()

    first >> second >> third

xcoms_kwargs_dag()