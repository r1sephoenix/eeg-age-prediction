"""
Core pipeline functions for product groups processing.
This module contains the shared logic used by both the CLI and Airflow DAG.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import project modules
from src.product_groups.functions import get_connection, get_data_gp
from src.product_groups.functions import estim_group_sales, normalize_groups, add_group_info
from src.product_groups.sql import _get_checkout_items, _get_receipt_data, _get_items_data


def fetch_checkout_items(store_id: str, start_date: str, end_date: str, connection=None) -> List[int]:
    """
    Fetch checkout items to exclude for a store.
    
    Args:
        store_id: Store ID to process
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        connection: Optional database connection object
        
    Returns:
        List of checkout item IDs to exclude
    """
    if connection is None:
        connection = get_connection("gp")
        
    checkout_query = _get_checkout_items(start_date, end_date, store_id)
    checkout_items = get_data_gp(checkout_query, connection)
    return checkout_items['item'].unique().tolist()


def fetch_store_data(store_id: str, start_date: str, end_date: str, 
                    connection=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch receipt and items data for a store.
    
    Args:
        store_id: Store ID to process
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        connection: Optional database connection object
        
    Returns:
        Tuple of (receipt_data, items_data) DataFrames
    """
    if connection is None:
        connection = get_connection("gp")
    
    # Get checkout items to exclude
    checkout_items_list = fetch_checkout_items(store_id, start_date, end_date, connection)
    
    # Get receipt data
    receipt_query = _get_receipt_data(checkout_items_list, start_date, end_date, store_id)
    receipt_data = get_data_gp(receipt_query, connection)
    
    # Get items data for enrichment
    items_query = _get_items_data()
    items_data = get_data_gp(items_query, connection)
    
    return receipt_data, items_data


def process_store_data(receipt_data: pd.DataFrame, items_data: pd.DataFrame, 
                      min_sales: int, min_group_length: int) -> pd.DataFrame:
    """
    Process store data to generate product groups.
    
    Args:
        receipt_data: DataFrame with receipt data
        items_data: DataFrame with items data
        min_sales: Minimum sales threshold
        min_group_length: Minimum group length
        
    Returns:
        DataFrame with processed product groups
    """
    # 1. Find groups with sufficient sales
    groups_with_sales = estim_group_sales(receipt_data, min_sales)
    
    # 2. Normalize groups
    normalized_groups = normalize_groups(groups_with_sales)
    
    # 3. Add group information
    enriched_groups = add_group_info(normalized_groups, items_data)
    
    # 4. Filter by minimum group length
    final_groups = enriched_groups[enriched_groups['gr_len'] >= min_group_length]
    
    return final_groups


def process_single_store(store_id: str, start_date: str, end_date: str, 
                         min_sales: int, min_group_length: int, 
                         output_path: Optional[Path] = None,
                         connection=None) -> Dict[str, Any]:
    """
    Process a single store - fetch data and generate product groups.
    
    Args:
        store_id: Store ID to process
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        min_sales: Minimum sales threshold
        min_group_length: Minimum group length
        output_path: Optional path to save results
        connection: Optional database connection object
        
    Returns:
        Dictionary with processing results
    """
    try:
        logger.info(f"Processing store {store_id}")
        
        if connection is None:
            connection = get_connection("gp")
        
        # Fetch data
        receipt_data, items_data = fetch_store_data(
            store_id, start_date, end_date, connection
        )
        
        # Process data
        final_groups = process_store_data(
            receipt_data, items_data, min_sales, min_group_length
        )
        
        # Save results if output path is provided
        if output_path:
            store_output_file = output_path / f"store_{store_id}_groups.csv"
            final_groups.to_csv(store_output_file, index=False)
            
        return {
            "store_id": store_id,
            "status": "success",
            "groups_count": len(final_groups['comb_id'].unique()),
            "groups_data": final_groups
        }
    
    except Exception as e:
        logger.error(f"Error processing store {store_id}: {str(e)}")
        return {
            "store_id": store_id,
            "status": "error",
            "error": str(e)
        }


def get_all_store_ids(connection=None) -> List[str]:
    """
    Get all active store IDs from the database.
    
    Args:
        connection: Optional database connection object
        
    Returns:
        List of store IDs
    """
    if connection is None:
        connection = get_connection("gp")
        
    all_stores_query = "SELECT DISTINCT store_id FROM assortment_ods.store WHERE store_type = 'store'"
    store_df = get_data_gp(all_stores_query, connection)
    return store_df['store_id'].astype(str).tolist()
