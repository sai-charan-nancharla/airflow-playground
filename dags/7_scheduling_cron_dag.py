from airflow.sdk import dag, task;
from pendulum import datetime;
from airflow.timetables.trigger import CronTriggerTimetable;

# start_date is required to start the DAG
# end_date is optional and can be used to stop the DAG from running after a certain date.
# catchup is set to True to allow the DAG to run for all the past scheduled intervals since the start_date if it was not running during that time.
# schedule = cron is to triiger cron job
# is_paused_upon_creation is set to False to make the DAG active upon creation unless paused.

@dag(
    dag_id="scheduling_cron_dag",
    start_date=datetime(
        year=2026,
        month=6,
        day=1,
        tz="Asia/Kolkata",
    ),
    schedule=CronTriggerTimetable("30 22 * * MON-FRI", timezone="Asia/Kolkata"),  # Cron expression for 10:30 PM on weekdays in Asia/Kolkata timezone
    is_paused_upon_creation=False,
    catchup=False
)

def scheduling_cron_dag():

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

    @task.python
    def publish_data(all_loaded_data): 
        print("Publishing data...")
        combined_data = {'s3_data': all_loaded_data[0], 'redshift_data': all_loaded_data[1], 'postgres_data': all_loaded_data[2], 'hold_publish': True}
        return combined_data;

    @task.branch
    def decide_publish(publish_data):
        if publish_data['hold_publish']:
            return 'skip_publish'
        else:
            return 'published_data'

    @task.bash
    def skip_publish():
        print("Holding publish step.")
        return 'echo "Publish was on Hold."'
    
    @task.bash
    def published_data():
        print("Published data Successfully.")
        return 'echo "Data published successfully."'

    t1 = extract_data()
    t2 = load_data_s3(t1)
    t3 = load_data_redshift(t1)
    t4 = load_data_postgres(t1)
    t5 = publish_data([t2, t3, t4])
    t6 = decide_publish(t5)
    t7 = skip_publish()
    t8 = published_data()

    # Define the branching logic -
    # Here list of [t7, t8] is not same as [t2, t3, t4] because t7 and t8 are the two possible paths after the branching decision.
    # t6 decides which path to take based on the output of t5 (publish_data)
    t1 >> [t2, t3, t4] >> t5 >> t6 >> [t7, t8]

scheduling_cron_dag()