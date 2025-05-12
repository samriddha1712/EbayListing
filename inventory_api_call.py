import requests
import urllib.parse
import base64
import re
import time
from supabase import create_client, Client
import os
from calculate_price import inclusive_price

# Configuration (unchanged)
table_name = os.getenv('SUPABASE_TABLE_NAME')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
EBAY_CREDENTIALS = {
    'client_id': os.getenv('EBAY_CLIENT_ID'),
    'client_secret': os.getenv('EBAY_CLIENT_SECRET'),
    'dev_id': os.getenv('EBAY_DEV_ID'),
    'redirect_uri': os.getenv('EBAY_REDIRECT_URI'),
    'merchant_location_key': os.getenv('EBAY_MERCHANT_LOCATION_KEY'),
    'business_policies': {
        'return': '245006369024',
        'payment': os.getenv('EBAY_PAYMENT_POLICY_ID'),
        'shipping': '245006370024'
    }
}

# Binding Type Short Codes (unchanged)
BINDING_SHORTCODES = {
    'Album': 'ALB',
    'Audio Cassette': 'ACS',
    'Audio CD': 'ACD',
    'Bath Book': 'BTH',
    'Blu-ray': 'BRY',
    'Board book': 'BBK',
    'board_book': 'BBK',
    'Bonded Leather': 'BLE',
    'Calendar': 'CAL',
    'Card Book': 'CRB',
    'Cards': 'CRD',
    'CD-ROM': 'CDR',
    'Diary': 'DIR',
    'DVD': 'DVD',
    'DVD Audio': 'DVA',
    'DVD-ROM': 'DVR',
    'Flexibound': 'FB',
    'Game': 'GAM',
    'Hardcover': 'HC',
    'Hardcover-spiral': 'HCS',
    'Imitation Leather': 'IML',
    'JP Oversized': 'JPO',
    'Kindle Edition': 'KIN',
    'Kindle Edition with Audio/Video': 'KAV',
    'Kitchen': 'KIT',
    'Leather Bound': 'LB',
    'Library Binding': 'LIB',
    'Loose Leaf': 'LSL',
    'Map': 'MAP',
    'Mass Market Paperback': 'MMP',
    'mass_market': 'MMP',
    'Misc.': 'MSC',
    'Misc. Supplies': 'MSC',
    'Notebook': 'NTB',
    'Novelty Book': 'NOV',
    'Office Product': 'OFP',
    'Pamphlet': 'PAM',
    'Paperback': 'PB',
    'Paperback Bunko': 'PBU',
    'Perfect Paperback': 'PPB',
    'Pocket Book': 'PKB',
    'Poster': 'POS',
    'print': 'PRT',
    'Print on Demand (Paperback)': 'POD',
    'Printed Access Code': 'PAC',
    'Product Bundle': 'PRB',
    'Rag Book': 'RGB',
    'Ring-bound': 'RNG',
    'School & Library Binding': 'SLB',
    'Sheet music': 'STM',
    'Spiral-bound': 'SPI',
    'Sports': 'SPT',
    'Staple Bound': 'STB',
    'Stationery': 'STN',
    'Textbook Binding': 'TXB',
    'Toy': 'TOY',
    'Unbound': 'UBD',
    'Unknown Binding': 'UNK',
    'VHS Tape': 'VHS',
    'Wall Chart': 'WCH'
}

# Existing functions (unchanged)
def generate_book_title(book_name, author, binding_type=None, publication_year=None, binding_codes=None):
    binding_codes = binding_codes or {}
    ellipsis = "…"
    
    def truncate(text, max_len):
        return text[:max_len-1] + ellipsis if len(text) > max_len else text

    binding_abbr = None
    if binding_type:
        if binding_type == 'Paperback':
            binding_abbr = 'PB'
        elif binding_type == 'Hardcover':
            binding_abbr = 'HC'
        else:
            binding_abbr = binding_codes.get(binding_type, binding_type)

    components = {
        'book': book_name,
        'by': "by",
        'author': author,
        'binding': binding_abbr,
        'year': publication_year
    }

    variations = []
    variations.append(f"{book_name} by {author} {binding_type or ''} Book {publication_year or ''}".strip())
    variations.append(f"{book_name} by {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    variations.append(f"{book_name} {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    base_length = len(f"{book_name} ") + len(f" {binding_abbr or ''} Book {publication_year or ''}".strip()) + 1
    max_author_len = 65 - base_length - 1
    if max_author_len >= 1:
        truncated_author = truncate(author, max_author_len)
        variations.append(f"{book_name} {truncated_author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    base_length = len(f" {author} {binding_abbr or ''} Book {publication_year or ''}".strip()) + 1
    max_book_len = 65 - base_length - 1
    if max_book_len >= 1:
        truncated_book = truncate(book_name, max_book_len)
        variations.append(f"{truncated_book} {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    for title in variations:
        if len(title) <= 65:
            return title[:65]
    
    return truncate(book_name, 62).ljust(65, ellipsis)[:65]

def extract_year(date_str):
    match = re.search(r'\b\d{4}\b', str(date_str))
    return match.group(0) if match else None

def calculate_start_price(item, final_price):
    try:
        vat_percent = (item['vat_code']).lower()
        if vat_percent == 'z':
            vat_percent = float(0.0)
        elif vat_percent == 's':
            vat_percent = float(20.0)
        else:
            vat_percent = float(5.0)

        if any(val < 0 for val in [vat_percent]):
            print(f"Invalid negative value in pricing data for item {item.get('id')}")
            return None

        net_price = final_price * (1 + (vat_percent / 100))
        return round(net_price, 2)
    
    except KeyError as e:
        print(f"Missing required pricing field {e} for item {item.get('id')}")
        return None
    except (TypeError, ValueError) as e:
        print(f"Invalid pricing data for item {item.get('id')}: {str(e)}")
        return None

def main():
    # Initialize Supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # eBay OAuth Flow (unchanged)
    auth_url = f"https://auth.ebay.com/oauth2/authorize?client_id={EBAY_CREDENTIALS['client_id']}&redirect_uri={urllib.parse.quote(EBAY_CREDENTIALS['redirect_uri'])}&response_type=code&scope=https://api.ebay.com/oauth/api_scope/sell.inventory"
    print(f"Authorize here: {auth_url}")
    redirect_url = input("Paste redirect URL after authorization: ")
    
    code = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_url).query)['code'][0]
    
    token_response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64.b64encode(f"{EBAY_CREDENTIALS['client_id']}:{EBAY_CREDENTIALS['client_secret']}".encode()).decode()
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": EBAY_CREDENTIALS['redirect_uri']
        }
    )
    access_token = token_response.json()['access_token']

    # HTTP headers for Inventory API
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Content-Language": "en-US"
    }

    success_count = 0
    inventory = supabase.table(table_name).select('*').execute().data
    for item in inventory:
        try:
            language = item.get('language', 'en')
            if language == 'en':
                language = 'English'

            binding = item.get('binding', 'unknown')
            pub_year = extract_year(item.get('publication_year'))
            title = generate_book_title(
                item['title'],
                item['author'], binding,
                pub_year, BINDING_SHORTCODES
            )

            sku = item['isbn13']
            inventory_item_payload = {
            "sku": sku,
            "condition": "NEW",
            "locale": "en_GB",
            "product": {
                "title": title,  # Ensure you have a valid title
                "description": item.get('description', 'No description available'),
                "imageUrls": [item['cover_image']] if item.get('cover_image') else [],
                "aspects": {
                    "Author": [item.get('author', 'Unknown Author')],
                    "Binding": [item.get('binding', 'Unknown Binding')],
                    "Language": [item.get('language', 'English')],
                    "Publication Year": [item['publication_year']],
                    "Publisher": [item.get('publisher', 'Unknown Publisher')],
                    "ISBN": [item['isbn13']]
                }
            },
            "availability": {
                "shipToLocationAvailability": {
                    "quantity": item['stock']
                }
            }
        }

            # Create or update inventory item
            inventory_url = f"https://api.ebay.com/sell/inventory/v1/inventory_item/{sku}"
            response = requests.put(inventory_url, headers=headers, json=inventory_item_payload)
            
            if response.status_code != 204:
                print(f"Error creating inventory item for {sku}: {response.text}")
                continue
            print("Success Listed")
            calculated_price_vat_exclusive = inclusive_price(item)
            calculated_price = calculate_start_price(item, calculated_price_vat_exclusive)
            if not calculated_price or calculated_price < 0.99:
                print(f"Skipping item {item.get('id')} - invalid price calculation")
                continue

            offer_payload = {
                "sku": sku,
                "marketplaceId": "EBAY_GB",
                "format": "FIXED_PRICE",
                "availableQuantity": item['stock'],
                "categoryId": "268",
                "listingPolicies": {
                    "paymentPolicyId": EBAY_CREDENTIALS['business_policies']['payment'],
                    "returnPolicyId": EBAY_CREDENTIALS['business_policies']['return'],
                    "fulfillmentPolicyId": EBAY_CREDENTIALS['business_policies']['shipping']
                },
                "pricingSummary": {
                    "price": {
                        "value": f"{calculated_price:.2f}",
                        "currency": "GBP"
                    }
                },
                "merchantLocationKey": EBAY_CREDENTIALS['merchant_location_key']
            }

            # Create offer
            offer_url = "https://api.ebay.com/sell/inventory/v1/offer"
            response = requests.post(offer_url, headers=headers, json=offer_payload)
            if response.status_code == 201:
                offer_id = response.json()['offerId']
            else:
                print(f"Error creating offer for {sku}: {response.text}")
                continue

            # Publish offer
            publish_url = f"https://api.ebay.com/sell/inventory/v1/offer/{offer_id}/publish"
            response = requests.post(publish_url, headers=headers)
            if response.status_code == 200:
                listing_id = response.json()['listingId']
                print(f"Successfully listed: {item['title']} (Listing ID: {listing_id})")
                success_count += 1
            else:
                print(f"Error publishing offer for {sku}: {response.text}")

            time.sleep(1)  # Rate limit

        except Exception as e:
            print(f"Failed to process item {item.get('id')}: {str(e)}")
            continue

if __name__ == "__main__":
    main()