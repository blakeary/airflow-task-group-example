from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


@dag(
    dag_id="task_group_example",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
)
def task_group_example():
    """Simple example of task groups"""
    
    # Start and end tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Define task groups for processing data
    groups = []
    for g_id in range(1, 4):
        @task_group(group_id=f"group{g_id}")
        def process_group():
            """A group of tasks that process data"""
            
            # First task in group
            @task
            def extract():
                return {"data": f"sample_data_for_group_{g_id}"}
            
            # Second task in group
            @task
            def transform(extracted_data):
                return {"transformed": f"transformed_{extracted_data['data']}"}
            
            # Third task in group
            @task
            def load(transformed_data):
                print(f"Loading: {transformed_data['transformed']}")
                return f"Loaded {transformed_data['transformed']}"
            
            # Set up the task dependencies within the group
            extracted = extract()
            transformed = transform(extracted)
            loaded = load(transformed)
            
            # Return the result from the last task
            return loaded
        
        # Add this group to our list
        groups.append(process_group())
    
    # Set dependencies between groups
    # Group 3 depends on groups 1 and 2
    [groups[0], groups[1]] >> groups[2]
    
    # Connect start and end
    start >> [groups[0], groups[1]]
    groups[2] >> end


# Instantiate the DAG
task_group_example()