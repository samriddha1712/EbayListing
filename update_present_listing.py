from supabase import create_client
import os

# Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
TABLE_NAME = "listing_phase2"
ISBN_FILE_PATH = "listed_isbns.txt"

# Connect to Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connected to Supabase")

# Read ISBNs from file
with open(ISBN_FILE_PATH, 'r') as file:
    isbn_list = [line.strip() for line in file if line.strip()]

# Process each ISBN
for isbn in isbn_list:
    # Check if ISBN exists
    result = supabase.table(TABLE_NAME).select("id").eq("isbn13", isbn).execute()
    
    if result.data:
        # Update 'listed' column to TRUE
        update_res = supabase.table(TABLE_NAME).update({"listed": True}).eq("isbn13", isbn).execute()
        print(f"Updated: {isbn}")
    else:
        print(f"Not found: {isbn}")
