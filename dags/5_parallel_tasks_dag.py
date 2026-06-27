from airflow.sdk import dag, task;


@dag(
    dag_id="parallel_tasks_dag"
)

def parallel_tasks_dag():

    @task.python
    def extract_data(**kwargs):
        print("Extracting data...")
        task_instance = kwargs['ti']
        extracted_data = {'extracted_data': [1, 2, 3, 4, 5]}  # Sample extracted data
        task_instance.xcom_push(key='extract_data', value=extracted_data)

    @task.python
    def load_data_s3(**kwargs):
        task_instance = kwargs['ti']
        extracted_data = task_instance.xcom_pull(key='extract_data', task_ids='extract_data')
        print(f"Loading data to S3: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*2}  # Example transformation
        task_instance.xcom_push(key='loaded_data', value=loaded_data)

    @task.python
    def load_data_redshift(**kwargs):
        task_instance = kwargs['ti']
        extracted_data = task_instance.xcom_pull(key='extract_data', task_ids='extract_data')
        print(f"Loading data to Redshift: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*3}  # Example transformation
        task_instance.xcom_push(key='loaded_data', value=loaded_data)

    @task.python
    def load_data_postgres(**kwargs):
        task_instance = kwargs['ti']
        extracted_data = task_instance.xcom_pull(key='extract_data', task_ids='extract_data')
        print(f"Loading data to Postgres: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*4}  # Example transformation
        task_instance.xcom_push(key='loaded_data', value=loaded_data)

    @task.bash
    def publish_data(**kwargs): 
        print("Publishing data...")
        task_instance = kwargs['ti']
        all_loaded_data = task_instance.xcom_pull(key='loaded_data', task_ids=['load_data_s3', 'load_data_redshift', 'load_data_postgres'])
        echoed_data = [data['loaded_data'] for data in all_loaded_data if data is not None]
        return f'echo "All loaded data: {echoed_data}"'

    t1 = extract_data()
    t2 = load_data_s3()
    t3 = load_data_redshift()
    t4 = load_data_postgres()
    t5 = publish_data()

    # Set dependencies to run tasks in parallel
    t1 >> [t2, t3, t4] >> t5

parallel_tasks_dag()