import typer
from typing import List, Optional
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# Import project modules
from src.product_groups.functions import get_connection, get_data_gp
from src.product_groups.functions import estim_group_sales, normalize_groups, add_group_info
from src.product_groups.sql import _get_checkout_items, _get_receipt_data, _get_items_data

app = typer.Typer(help="Product Groups CLI for running calculations across stores")

from src.product_groups.pipeline import (
    process_single_store, 
    get_all_store_ids
)

@app.command()
def process_stores(
    store_ids: List[str] = typer.Argument(None, help="List of store IDs to process"),
    start_date: str = typer.Option(None, "--start-date", "-s", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date (YYYY-MM-DD)"),
    min_sales: int = typer.Option(5, "--min-sales", "-m", help="Minimum sales threshold"),
    min_group_length: int = typer.Option(3, "--min-length", "-l", help="Minimum group length"),
    output_dir: str = typer.Option("./output", "--output", "-o", help="Output directory for results"),
    parallel: bool = typer.Option(False, "--parallel", "-p", help="Run in parallel")
):
    """Process product group calculations for specified stores or all stores.
    
    If no store IDs are provided, runs for all active stores.
    """
    typer.echo("Starting product group calculations...")
    
    # Get connection
    connection = get_connection("gp")
    
    # If no store IDs provided, get all stores
    if not store_ids:
        typer.echo("No stores specified, fetching all store IDs...")
        store_ids = get_all_store_ids(connection)
        typer.echo(f"Found {len(store_ids)} active stores")
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Set default dates if not provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Process stores
    if parallel:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            results = list(executor.map(
                lambda store_id: process_single_store(
                    store_id, start_date, end_date, min_sales, min_group_length, output_path, connection
                ), 
                store_ids
            ))
    else:
        results = []
        for store_id in typer.progressbar(store_ids, label="Processing stores"):
            result = process_single_store(
                store_id, start_date, end_date, min_sales, min_group_length, output_path, connection
            )
            results.append(result)
    
    # Summarize results
    successful = sum(1 for r in results if r['status'] == 'success')
    typer.echo(f"Processed {len(results)} stores: {successful} successful, {len(results) - successful} failed")

@app.command()
def list_stores():
    """List all available stores that can be processed."""
    connection = get_connection("gp")
    # Query to get all active store IDs with names
    stores_query = "SELECT DISTINCT store_id, store_name FROM assortment_ods.store WHERE store_type = 'store'"
    stores = get_data_gp(stores_query, connection)
    
    typer.echo("Available stores:")
    for _, row in stores.iterrows():
        typer.echo(f"ID: {row['store_id']}\tName: {row['store_name']}")

@app.command()
def run_airflow_pipeline(
    store_ids: List[str] = typer.Argument(None, help="List of store IDs to process in Airflow"),
    start_date: str = typer.Option(None, "--start-date", "-s", help="Start date (YYYY-MM-DD)"),
    end_date: str = typer.Option(None, "--end-date", "-e", help="End date (YYYY-MM-DD)"),
    min_sales: int = typer.Option(5, "--min-sales", "-m", help="Minimum sales threshold"),
    min_group_length: int = typer.Option(3, "--min-length", "-l", help="Minimum group length"),
    output_dir: str = typer.Option(None, "--output", "-o", help="Output directory for results"),
    dag_id: str = typer.Option("product_groups_pipeline", "--dag-id", "-d", help="Airflow DAG ID")
):
    """Trigger Airflow DAG run with specified store IDs and parameters.
    
    If no store IDs are provided, the DAG will run for all active stores.
    """
    try:
        from airflow.api.client.local_client import Client
        
        airflow_client = Client(None, None)
        
        # Prepare conf dictionary to pass to Airflow
        conf = {}
        
        if store_ids:
            # Convert store IDs list to comma-separated string
            store_ids_str = ",".join(store_ids)
            conf["store_ids"] = store_ids_str
            typer.echo(f"Running Airflow DAG for stores: {store_ids_str}")
        else:
            typer.echo("Running Airflow DAG for all active stores")
        
        # Add other parameters if provided
        if start_date:
            conf["start_date"] = start_date
        if end_date:
            conf["end_date"] = end_date
        if min_sales:
            conf["min_sales"] = str(min_sales)
        if min_group_length:
            conf["min_group_length"] = str(min_group_length)
        if output_dir:
            conf["output_dir"] = output_dir
        
        # Trigger the DAG
        execution_date = datetime.now().isoformat()
        airflow_client.trigger_dag(dag_id=dag_id, run_id=f"manual_{execution_date}", conf=conf)
        
        typer.echo(f"Successfully triggered DAG '{dag_id}'")
        typer.echo(f"Check Airflow UI for execution status")
        
    except ImportError:
        typer.echo("Error: Airflow client not available. Make sure Airflow is properly installed.")
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Error triggering Airflow DAG: {str(e)}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()
