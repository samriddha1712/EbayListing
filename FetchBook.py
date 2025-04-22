import os
import csv
import logging
import time
import json
from typing import List, Dict, Set, Optional
from datetime import datetime
import requests
from ftplib import FTP_TLS
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from tqdm import tqdm
from upload_supa import upload_csv_to_supabase

# Load environment variables
load_dotenv()
logging.basicConfig(level=logging.INFO)

# Configuration
FTP_URL = os.getenv('FTP_HOST')
FTP_USER = os.getenv('FTP_USER')
FTP_PASS = os.getenv('FTP_PASS')
ISBNDB_API_KEY = os.getenv('ISBNDB_API_KEY')

BATCH_SIZE = 100
MAX_API_RETRIES = 3
API_RETRY_DELAY = 2


def download_ftp_files() -> List[str]:
    """Download all txt files from FTP inventory folder"""
    downloaded_files = []
    try:
        with FTP_TLS(FTP_URL) as ftp:
            ftp.login(FTP_USER, FTP_PASS)
            ftp.cwd('/inventory')
            
            files = [f for f in ftp.nlst() if f.upper().endswith('.TXT')]
            if not files:
                tqdm.write("No TXT files found in FTP directory")
                return []

            with tqdm(files, desc="Downloading files", unit="file") as pbar:
                for filename in pbar:
                    local_path = f"downloaded/{filename}"
                    os.makedirs('downloaded', exist_ok=True)
                    
                    with open(local_path, 'wb') as f:
                        ftp.retrbinary(f'RETR {filename}', f.write)
                    downloaded_files.append(local_path)
                    pbar.set_postfix(file=filename[:15])
    except Exception as e:
        logging.error(f"FTP Error: {str(e)}")
    return downloaded_files

def normalize_column_name(name: str) -> str:
    """Convert column name to lowercase and sanitize"""
    return name.strip().lower()

def process_files(file_paths: List[str]) -> tuple[List[Dict], Set[str], Set[str]]:
    """Process CSV files, filter valid rows, and collect all columns"""
    all_rows = []
    ean_set = set()
    all_input_columns = set()
    

    with tqdm(file_paths, desc="Processing files", unit="file") as pbar:
        for file_path in pbar:
            try:
                with open(file_path, 'r') as f:
                    reader = csv.reader(f)
                    next(reader)  # Skip HEADER row
                    headers = [normalize_column_name(h) for h in next(reader)]
                    all_input_columns.update(headers)
                    
                    try:
                        stock_idx = headers.index('stock')
                    except ValueError:
                        logging.error(f"'stock' column missing in {file_path}")
                        continue

                    row_count = 0
                    for row in tqdm(reader, desc=f"Processing {os.path.basename(file_path)}", leave=False):
                        
                        if len(row) <= stock_idx or int(row[stock_idx].strip()) <= 4:
                            continue
                        
                        row_dict = {headers[i]: val.strip() for i, val in enumerate(row)}
                        all_rows.append(row_dict)
                        ean_set.add(row_dict['ean'])
                        row_count += 1
                    
                    pbar.set_postfix(rows=row_count, file=os.path.basename(file_path))
            except Exception as e:
                logging.error(f"Error processing {file_path}: {str(e)}")

    tqdm.write(f"Processed {len(all_rows)} valid records from {len(file_paths)} files")
    return all_rows, ean_set, all_input_columns

@retry(stop=stop_after_attempt(MAX_API_RETRIES), 
       wait=wait_exponential(multiplier=1, min=API_RETRY_DELAY, max=30))
def fetch_bulk_book_details(identifiers: List[str], id_type: str) -> Dict[str, Optional[Dict]]:
    """Bulk fetch book details with proper JSON handling"""
    if not identifiers:
        return {}

    url = "https://api2.isbndb.com/books"
    headers = {
        "Authorization": ISBNDB_API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        payload = 'isbns=' + ','.join(identifiers)
        response = requests.post(url, headers=headers, data=payload, timeout=30)
        response.raise_for_status()
        
        json_response = response.json()
        books = json_response.get("data", [])
        
        return {
            str(book[id_type]).strip(): process_book_data(book)
            for book in books
            if book.get(id_type)
        }
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            retry_after = int(e.response.headers.get('Retry-After', 30))
            tqdm.write(f"Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            raise
        logging.error(f"HTTP Error {e.response.status_code}: {e.response.text}")
        return {}
    except Exception as e:
        logging.error(f"API Request failed: {str(e)}")
        return {}

def process_book_data(book: Dict) -> Dict:
    """Normalize ISBNdb API response structure"""
    return {
        "title": book.get("title"),
        "description": book.get("synopsis") or book.get("description"),
        "cover_image": book.get("image"),
        "author": ", ".join(book.get("authors", [])),
        "publisher": book.get("publisher"),
        "publication_year": book.get("date_published"),
        "language": book.get("language"),
        "isbn10": book.get("isbn10"),
        "isbn13": book.get("isbn13"),
        "pages": book.get("pages"),
        "binding": book.get("binding")
    }

def fetch_book_data(eans: List[str]) -> Dict[str, Dict]:
    """Orchestrate batch API requests with fail-safes"""
    api_data = {}
    total_eans = len(eans)
    
    with tqdm(total=total_eans, desc="Fetching book data", unit="EAN") as pbar:
        batch_num = 0
        while batch_num * BATCH_SIZE < total_eans:
            try:
                start_idx = batch_num * BATCH_SIZE
                end_idx = start_idx + BATCH_SIZE
                batch = eans[start_idx:end_idx]
                
                result = fetch_bulk_book_details(batch, "isbn13")
                api_data.update(result)
                
                pbar.update(len(batch))
                batch_num += 1
                
                # time.sleep(0.5)
                
            except Exception as e:
                logging.error(f"Batch {batch_num} failed: {str(e)}")
                if batch_num * BATCH_SIZE >= total_eans:
                    break
                continue
    
    tqdm.write(f"API processing completed. Matched {len(api_data)}/{total_eans} EANs")
    return api_data

def main():
    """Main processing pipeline with CSV output only"""
    try:
        files = download_ftp_files()
        
        if not files:
            raise ValueError("No input files specified")

        os.makedirs('output', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        matched_csv = f"output/matched_{timestamp}.csv"
        unmatched_csv = f"output/unmatched_{timestamp}.csv"

        # Process files and collect all columns
        all_rows, all_eans, all_input_columns = process_files(files)
        api_data = fetch_book_data(list(all_eans))

        # Define API fields
        api_fields = {'title', 'description', 'cover_image', 'author', 'publisher', 
                      'publication_year', 'language', 'isbn10', 'isbn13', 'pages', 'binding'}
        matched_fieldnames = sorted(all_input_columns | api_fields)
        unmatched_fieldnames = sorted(all_input_columns)

        # Collect data in memory
        matched_data = []
        unmatched_data = []

        with tqdm(total=len(all_rows), desc="Processing records") as pbar:
            for row in all_rows:
                try:
                    ean = row['ean'].strip()
                    if ean in api_data:
                        book_data = api_data[ean]
                        merged = {k.lower(): v for k, v in {**row, **book_data}.items() 
                                if v not in (None, "", "null")}
                        matched_data.append(merged)
                    else:
                        clean_row = {k.lower(): v for k, v in row.items()}
                        unmatched_data.append(clean_row)
                    pbar.update(1)
                except Exception as e:
                    logging.error(f"Error processing record: {str(e)}")
                    continue

        # Write matched CSV with consistent columns
        if matched_data:
            with open(matched_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=matched_fieldnames, restval='')
                writer.writeheader()
                writer.writerows(matched_data)

        # Write unmatched CSV with consistent columns
        if unmatched_data:
            with open(unmatched_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=unmatched_fieldnames, restval='')
                writer.writeheader()
                writer.writerows(unmatched_data)

        tqdm.write(f"\nProcessing completed")
        tqdm.write(f"Matched records: {len(matched_data)} ({os.path.abspath(matched_csv)})")
        tqdm.write(f"Unmatched records: {len(unmatched_data)} ({os.path.abspath(unmatched_csv)})")
        
        upload_csv_to_supabase(matched_csv)

    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
        raise

if __name__ == '__main__':
    main()