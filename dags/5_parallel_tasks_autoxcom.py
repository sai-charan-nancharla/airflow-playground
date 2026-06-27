from airflow.sdk import dag, task;


@dag(
    dag_id="parallel_tasks_autoxcom"
)

def parallel_tasks_autoxcom():

    @task.python
    def extract_data():
        print("Extracting data...")
        extracted_data = {'extracted_data': [1, 2, 3, 4, 5]}  # Sample extracted data
        return extracted_data  # Auto XCom push
    
    @task.python
    def load_data_s3(extracted_data):
        print(f"Loading data to S3: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*2}  # Example transformation
        return loaded_data

    @task.python
    def load_data_redshift(extracted_data):
        print(f"Loading data to Redshift: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*3}  # Example transformation
        return loaded_data

    @task.python
    def load_data_postgres(extracted_data):
        print(f"Loading data to Postgres: {extracted_data}")
        loaded_data = {'loaded_data': extracted_data['extracted_data']*4}  # Example transformation
        return loaded_data

    @task.bash
    def publish_data(all_loaded_data): 
        print("Publishing data...")
        echoed_data = [data['loaded_data'] for data in all_loaded_data if data is not None]
        return f'echo "All loaded data: {echoed_data}"'

    t1 = extract_data()
    t2 = load_data_s3(t1)
    t3 = load_data_redshift(t1)
    t4 = load_data_postgres(t1)
    t5 = publish_data([t2, t3, t4])

parallel_tasks_autoxcom()