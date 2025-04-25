from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
from pathlib import Path

# Add project root directory to path
sys.path.append(str(Path(__file__).parents[2]))

# Import project modules
from src.product_groups.functions import get_connection, get_data_gp, get_data_ch
from src.product_groups.functions import estim_group_sales, normalize_groups, add_group_info
from src.product_groups.sql import _get_checkout_items, _get_receipt_data, _get_items_data

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define helper functions
def get_store_list(**context):
    """Get list of active stores from the database"""
    # Use the shared function
    store_ids = get_all_store_ids()
    
    # Push store IDs to XCom for downstream tasks
    context['task_instance'].xcom_push(key='store_ids', value=store_ids)
    return store_ids

# Import the shared pipeline module
from src.product_groups.pipeline import (
    fetch_store_data, 
    process_store_data as process_data, 
    get_all_store_ids
)

def fetch_data_for_store(store_id, **context):
    """Fetch data for a specific store and prepare for processing"""
    # Get dates from context or use default
    execution_date = context['execution_date']
    dag_run = context.get('dag_run')
    
    # Check if dates were passed in DAG run config
    if dag_run and dag_run.conf:
        start_date = dag_run.conf.get('start_date', 
                                     (execution_date - timedelta(days=30)).strftime("%Y-%m-%d"))
        end_date = dag_run.conf.get('end_date', 
                                   execution_date.strftime("%Y-%m-%d"))
    else:
        start_date = (execution_date - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = execution_date.strftime("%Y-%m-%d")
    
    # Get connection
    connection = get_connection("gp")
    
    # Use shared function to fetch data
    receipt_data, items_data = fetch_store_data(
        store_id, start_date, end_date, connection
    )
    
    # Save intermediate results to be used by the process task
    temp_dir = Path(Variable.get("TEMP_DIR", "/tmp/airflow/product_groups"))
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    receipt_file = temp_dir / f"receipts_{store_id}.parquet"
    items_file = temp_dir / f"items_{store_id}.parquet"
    
    receipt_data.to_parquet(receipt_file)
    items_data.to_parquet(items_file)
    
    return {
        "store_id": store_id,
        "receipt_file": str(receipt_file),
        "items_file": str(items_file),
    }

def process_store_data(store_id, **context):
    """Process data for a specific store to generate product groups"""
    # Get parameters
    dag_run = context.get('dag_run')
    
    # Check if parameters were passed in DAG run config
    if dag_run and dag_run.conf:
        min_sales = int(dag_run.conf.get('min_sales', 
                                        Variable.get("MIN_SALES", "5")))
        min_length = int(dag_run.conf.get('min_group_length', 
                                         Variable.get("MIN_GROUP_LENGTH", "3")))
    else:
        min_sales = int(Variable.get("MIN_SALES", "5"))
        min_length = int(Variable.get("MIN_GROUP_LENGTH", "3"))
    
    # Get paths from previous task
    ti = context['task_instance']
    store_data = ti.xcom_pull(task_ids=f'fetch_data_for_store_{store_id}')
    
    receipt_file = store_data["receipt_file"]
    items_file = store_data["items_file"]
    
    # Load data
    receipt_data = pd.read_parquet(receipt_file)
    items_data = pd.read_parquet(items_file)
    
    # Process the data using the shared function
    final_groups = process_data(
        receipt_data, items_data, min_sales, min_length
    )
    
    # Save results
    output_dir = Path(Variable.get("OUTPUT_DIR", "/opt/airflow/data/output"))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"store_{store_id}_groups.csv"
    final_groups.to_csv(output_file, index=False)
    
    # Clean up temporary files
    os.remove(receipt_file)
    os.remove(items_file)
    
    return {
        "store_id": store_id,
        "status": "success",
        "groups_count": len(final_groups['comb_id'].unique()),
        "output_file": str(output_file)
    }

def save_results_to_s3(**context):
    """Upload results to S3 bucket"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        # Get S3 configuration
        s3_bucket = Variable.get("S3_BUCKET", "product-groups-results")
        s3_prefix = Variable.get("S3_PREFIX", "store_groups")
        execution_date = context['execution_date'].strftime("%Y-%m-%d")
        
        # Get output directory
        output_dir = Path(Variable.get("OUTPUT_DIR", "/opt/airflow/data/output"))
        
        # Connect to S3
        s3_client = boto3.client('s3')
        
        # Upload each file
        for file_path in output_dir.glob("*.csv"):
            s3_key = f"{s3_prefix}/{execution_date}/{file_path.name}"
            s3_client.upload_file(str(file_path), s3_bucket, s3_key)
            print(f"Uploaded {file_path.name} to s3://{s3_bucket}/{s3_key}")
        
        return {
            "status": "success",
            "s3_bucket": s3_bucket,
            "s3_prefix": f"{s3_prefix}/{execution_date}",
            "files_count": len(list(output_dir.glob("*.csv")))
        }
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

# Function to get store IDs from DAG run or config
def get_store_ids_for_processing(**context):
    """Get store IDs from DAG run configuration, Variable, or database"""
    # 1. Check if store_ids was passed directly in DAG run config
    dag_run = context.get('dag_run')
    
    if dag_run and dag_run.conf and 'store_ids' in dag_run.conf:
        store_ids_str = dag_run.conf.get('store_ids')
        if store_ids_str:
            store_ids = store_ids_str.split(',')
            context['task_instance'].xcom_push(key='store_ids', value=store_ids)
            print(f"Using store IDs from DAG run config: {store_ids}")
            return store_ids
    
    # 2. Check if store_ids is set in Airflow Variables
    try:
        store_ids_var = Variable.get("STORE_IDS", default_var=None)
        if store_ids_var and store_ids_var.strip():
            store_ids = [s.strip() for s in store_ids_var.split(',') if s.strip()]
            if store_ids:
                context['task_instance'].xcom_push(key='store_ids', value=store_ids)
                print(f"Using store IDs from Airflow Variable: {store_ids}")
                return store_ids
    except:
        pass
    
    # 3. Fallback to getting all store IDs from database
    connection = get_connection("gp")
    query = "SELECT DISTINCT store_id FROM assortment_ods.store WHERE store_type = 'store'"
    stores_df = get_data_gp(query, connection)
    store_ids = stores_df['store_id'].astype(str).tolist()
    
    context['task_instance'].xcom_push(key='store_ids', value=store_ids)
    print(f"Using all store IDs from database: {len(store_ids)} stores")
    return store_ids

# Create the DAG
with DAG(
    'product_groups_pipeline',
    default_args=default_args,
    description='Product Groups analysis pipeline for all stores or selected stores',
    schedule_interval='@weekly',
    catchup=False,
) as dag:
    
    # Task to get the list of stores to process
    get_stores_task = PythonOperator(
        task_id='get_store_ids',
        python_callable=get_store_ids_for_processing,
        provide_context=True,
    )
    
    # Dynamically create tasks for each store
    def create_store_tasks(store_id):
        # Task to fetch data for store
        fetch_task = PythonOperator(
            task_id=f'fetch_data_for_store_{store_id}',
            python_callable=fetch_data_for_store,
            op_kwargs={'store_id': store_id},
            provide_context=True,
        )
        
        # Task to process data for store
        process_task = PythonOperator(
            task_id=f'process_store_{store_id}',
            python_callable=process_store_data,
            op_kwargs={'store_id': store_id},
            provide_context=True,
        )
        
        # Set dependencies
        get_stores_task >> fetch_task >> process_task
        
        return process_task
    
    # Get a list of store IDs to create static tasks for
    # This is needed for Airflow to create the task graph at parse time
    # The actual store IDs used will be determined at runtime by get_store_ids_for_processing
    default_store_ids = Variable.get("DEFAULT_STORE_IDS", "").split(",")
    if not default_store_ids or (len(default_store_ids) == 1 and not default_store_ids[0]):
        default_store_ids = ["101", "102", "103"]  # Default store IDs for DAG visualization
    
    # Create tasks for each store
    store_tasks = [create_store_tasks(store_id) for store_id in default_store_ids]
    
    # Final task to save results to S3
    save_to_s3_task = PythonOperator(
        task_id='save_results_to_s3',
        python_callable=save_results_to_s3,
        provide_context=True,
    )
    
    # Set final dependency
    store_tasks >> save_to_s3_task
