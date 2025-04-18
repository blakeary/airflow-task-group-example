from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from pendulum import datetime
import random


@dag(
    dag_id="branch_group_example",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
)
def branch_group_example():
    """Example combining task groups, branch operators and XCom"""
    
    # Start and end tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    
    # Task to determine which processing path to take
    @task
    def check_data_source():
        # Simulate checking what kind of data we have
        sources = ["api", "database", "file"]
        selected_source = random.choice(sources)
        print(f"Selected data source: {selected_source}")
        return {"source": selected_source}
    
    # Branch function to determine which task to execute
    # Important: We must return the first task ID in each group, not the group ID itself
    def branch_func(ti=None):
        source = ti.xcom_pull(task_ids="check_data_source")["source"]
        if source == "api":
            return "api_group.extract_from_api"
        elif source == "database":
            return "database_group.extract_from_db" 
        else:
            return "file_group.extract_from_file"
    
    # Branch operator that will select one of the task groups
    branch = BranchPythonOperator(
        task_id="branch_to_source",
        python_callable=branch_func
    )
    
    # Define API processing task group
    @task_group(group_id="api_group")
    def process_api():
        @task
        def extract_from_api():
            return {"data": "api_data", "records": 100}
        
        @task
        def transform_api_data(extracted_data):
            return {
                "transformed": f"transformed_{extracted_data['data']}",
                "record_count": extracted_data["records"]
            }
        
        @task
        def load_api_data(transformed_data):
            print(f"Loading API data: {transformed_data}")
            return {"source": "api", "status": "success", "records": transformed_data["record_count"]}
        
        # Set up the task dependencies
        extracted = extract_from_api()
        transformed = transform_api_data(extracted)
        loaded = load_api_data(transformed)
        
        # Return the result
        return loaded
    
    # Define database processing task group
    @task_group(group_id="database_group")
    def process_database():
        @task
        def extract_from_db():
            return {"data": "database_data", "tables": 3}
        
        @task
        def transform_db_data(extracted_data):
            return {
                "transformed": f"transformed_{extracted_data['data']}",
                "table_count": extracted_data["tables"]
            }
        
        @task
        def load_db_data(transformed_data):
            print(f"Loading DB data: {transformed_data}")
            return {"source": "database", "status": "success", "tables": transformed_data["table_count"]}
        
        # Set up the task dependencies
        extracted = extract_from_db()
        transformed = transform_db_data(extracted)
        loaded = load_db_data(transformed)
        
        # Return the result
        return loaded
    
    # Define file processing task group
    @task_group(group_id="file_group")
    def process_file():
        @task
        def extract_from_file():
            return {"data": "file_data", "size_mb": 42}
        
        @task
        def transform_file_data(extracted_data):
            return {
                "transformed": f"transformed_{extracted_data['data']}",
                "size": extracted_data["size_mb"]
            }
        
        @task
        def load_file_data(transformed_data):
            print(f"Loading file data: {transformed_data}")
            return {"source": "file", "status": "success", "size_mb": transformed_data["size"]}
        
        # Set up the task dependencies
        extracted = extract_from_file()
        transformed = transform_file_data(extracted)
        loaded = load_file_data(transformed)
        
        # Return the result
        return loaded
    
    # Create task group instances
    api_process = process_api()
    db_process = process_database()
    file_process = process_file()
    
    # Task to summarize results from any path
    @task(trigger_rule="one_success")
    def summarize_results(**context):
        ti = context["ti"]
        
        # Try to pull XCom from all possible task groups
        results = []
        for task_id in ["api_group.load_api_data", "database_group.load_db_data", "file_group.load_file_data"]:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                results.append(result)
        
        # Process the results we got
        if results:
            source = results[0]["source"]
            print(f"Processed data from {source} source successfully")
            if source == "api":
                print(f"Processed {results[0]['records']} records")
            elif source == "database":
                print(f"Processed {results[0]['tables']} tables")
            else:
                print(f"Processed {results[0]['size_mb']}MB of file data")
        
        return {"processed": True, "source": results[0]["source"] if results else "unknown"}
    
    # Define the workflow
    data_source = check_data_source()
    
    # Set up the dependencies
    start >> data_source >> branch
    branch >> [api_process, db_process, file_process]
    
    # Only one of the processes will run based on the branch
    summarize = summarize_results()
    [api_process, db_process, file_process] >> summarize >> end


# Instantiate the DAG
branch_group_example()