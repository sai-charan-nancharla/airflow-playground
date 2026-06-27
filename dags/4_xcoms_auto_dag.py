from airflow.sdk import dag, task;

@dag(
    dag_id="xcoms_auto_dag"
)
def xcoms_auto_dag():

    @task.python
    def extract_task():
        print('Fetching Data');
        fetched_data = {"fetch_data": [1, 2, 3, 4, 5]}
        # For this return airflow automatically pushes the output to XComs with the key 'return_value' and the task_id of the task as the key. This is a new feature in Airflow 3.0 that allows for more intuitive data passing between tasks.
        return fetched_data 
    

    @task.python
    def transform_task(first_task_output):
        # Similarly when the output of the first task is passed as an argument to the second task
        # airflow automatically pulls the output from XComs and passes it to the second task. This is a more intuitive way to handle data passing between tasks compared to using XComs explicitly.
        print('Transforming Data');
        print(f"Received output from first task: {first_task_output}")
        transformed_data = {"transform_data": [x * 2 for x in first_task_output["fetch_data"]]}
        return transformed_data

    @task.python    
    def load_task(second_task_output):
        print('Loading Data');
        print(f"Received output from second task: {second_task_output}")
        return "Data loaded successfully"

    # Defining task dependencies
    # Since airflow 3.0, we can pass the output of one task directly to another task as an argument. 
    # This is a more intuitive way to handle data passing between tasks compared to using XComs explicitly.
    # Even this also updates the XComs automatically in the background, so we don't have to worry about it.
    # Since we define the input of task from other task's output, we don't have to define the dependencies explicitly. Airflow will automatically infer the dependencies based on the function calls.
    first = extract_task()
    second = transform_task(first)
    third = load_task(second)

xcoms_auto_dag()