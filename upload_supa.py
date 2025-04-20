import os
import csv
import json
import logging
from typing import List, Dict
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from supabase import create_client, Client
import time

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO)

# Configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
TABLE_NAME = os.getenv('SUPABASE_TABLE_NAME')
BATCH_SIZE = 100

def get_existing_columns(supabase: Client) -> List[str]:
    """Retrieve current table columns from Supabase"""
    try:
        result = supabase.table(TABLE_NAME).select("*").limit(1).execute()
        if result.data:
            return list(result.data[0].keys())
        return []
    except Exception as e:
        logging.error(f"Column detection error: {e}")
        return []

def check_and_create_table(supabase: Client, data_columns: List[str]):
    """Ensure table schema matches CSV structure"""
    try:
        supabase.table(TABLE_NAME).select("*").limit(1).execute()
        logging.info(f"Table {TABLE_NAME} exists")
        
        existing_columns = get_existing_columns(supabase)
        missing = [col for col in data_columns if col not in existing_columns]
        
        if missing:
            logging.info(f"Adding missing columns: {missing}")
            for col in missing:
                supabase.rpc('execute_sql', {
                    'sql': f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {col} TEXT;"
                }).execute()
            time.sleep(2)

    except Exception as e:
        if 'does not exist' in str(e):
            logging.info(f"Creating new table {TABLE_NAME}")
            cols = ",\n".join([f"{col} TEXT" for col in data_columns])
            supabase.rpc('execute_sql', {
                'sql': f"CREATE TABLE {TABLE_NAME} (id SERIAL PRIMARY KEY,\n{cols});"
            }).execute()
            time.sleep(5)
        else:
            raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def safe_batch_insert(supabase: Client, batch: List[Dict]) -> None:
    """Retryable batch insert operation"""
    supabase.table(TABLE_NAME).insert(batch).execute()

def upload_csv_to_supabase(csv_path: str) -> None:
    """Main upload function with progress tracking"""
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Connected to Supabase")

    # Read CSV file with UTF-8 encoding
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data_columns = [normalize_column_name(col) for col in reader.fieldnames]
        rows = [row for row in reader]

    check_and_create_table(supabase, data_columns)
    
    total_records = len(rows)
    inserted = 0
    failed_batches = []

    for batch_idx in range(0, total_records, BATCH_SIZE):
        batch = rows[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = (batch_idx // BATCH_SIZE) + 1

        try:
            safe_batch_insert(supabase, batch)
            inserted += len(batch)
            logging.info(f"Batch {batch_num} inserted ({inserted}/{total_records})")
            time.sleep(1)
        except RetryError as e:
            logging.error(f"Batch {batch_num} failed after retries: {e.last_attempt.exception()}")
            failed_batches.append(batch)
        except Exception as e:
            logging.error(f"Unexpected error in batch {batch_num}: {e}")
            failed_batches.append(batch)

    # Handle failed batches
    if failed_batches:
        timestamp = int(time.time())
        failed_file = f"failed_batches_{timestamp}.json"
        with open(failed_file, 'w') as f:
            json.dump(failed_batches, f)
        logging.error(f"Saved {len(failed_batches)} failed batches to {failed_file}")

    logging.info(f"Upload complete. Success: {inserted}/{total_records}")

def normalize_column_name(name: str) -> str:
    """Convert column name to lowercase and sanitize"""
    name = name.strip().lower().replace(' ', '_')
    name = ''.join([c if c.isalnum() or c == '_' else '_' for c in name])
    return name[:63] if name else 'unknown_column'


