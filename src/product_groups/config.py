from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
env_path = Path(__file__).parents[2] / '.env'
load_dotenv(dotenv_path=env_path)

# Default dates (current month and last year same month)
TODAY = datetime.now()
# Current period
SD_CUR = os.getenv('START_DATE_CURRENT', (TODAY.replace(day=1)).strftime('%Y-%m-%d'))
ED_CUR = os.getenv('END_DATE_CURRENT', TODAY.strftime('%Y-%m-%d'))

# Last year same period
LAST_YEAR = TODAY.replace(year=TODAY.year-1)
SD_LY = os.getenv('START_DATE_LASTYEAR', (LAST_YEAR.replace(day=1)).strftime('%Y-%m-%d'))
ED_LY = os.getenv('END_DATE_LASTYEAR', LAST_YEAR.strftime('%Y-%m-%d'))

# Default store ID
STORE_ID = os.getenv('STORE_ID', '101')

# Database connections
# ClickHouse
CH_HOST = os.getenv('CH_HOST', 'localhost')
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD', '')
CH_DATABASE = os.getenv('CH_DATABASE', 'default')

# GreenPlum
GP_HOST = os.getenv('GP_HOST', 'localhost')
GP_PORT = os.getenv('GP_PORT', '5432')
GP_USER = os.getenv('GP_USER', 'postgres')
GP_PASSWORD = os.getenv('GP_PASSWORD', 'postgres')
GP_DATABASE = os.getenv('GP_DATABASE', 'postgres')

# S3 Storage
S3_BUCKET = os.getenv('S3_BUCKET', 'product-groups-results')
S3_PREFIX = os.getenv('S3_PREFIX', 'store_groups')

# Model parameters
MIN_SALES = int(os.getenv('MIN_SALES', '5'))
MIN_GROUP_LENGTH = int(os.getenv('MIN_GROUP_LENGTH', '3'))

# Output directory
OUTPUT_DIR = os.getenv('OUTPUT_DIR', str(Path(__file__).parents[2] / 'output'))

# Temporary directory for processing
TEMP_DIR = os.getenv('TEMP_DIR', str(Path(__file__).parents[2] / 'temp'))
