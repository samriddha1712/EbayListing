import requests
import urllib.parse
import base64
import re
import time
from supabase import create_client, Client
from ebaysdk.trading import Connection
from ebaysdk.exception import ConnectionError
from ebaysdk.utils import dict2xml
import os


# Configuration
table_name = os.getenv('SUPABASE_TABLE_NAME')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
EBAY_CREDENTIALS = {
    'client_id': os.getenv('EBAY_CLIENT_ID'),
    'client_secret': os.getenv('EBAY_CLIENT_SECRET'),
    'dev_id': os.getenv('EBAY_DEV_ID'),
    'redirect_uri': os.getenv('EBAY_REDIRECT_URI'),
    
    'business_policies': {
        'return': '245006369024',
        'payment': os.getenv('EBAY_PAYMENT_POLICY_ID'),
        'shipping': '245006370024'
    }
}

# Binding Type Short Codes
BINDING_SHORTCODES = {
    # Exact match mappings
    'Album': 'ALB',
    'Audio Cassette': 'ACS',
    'Audio CD': 'ACD',
    'Bath Book': 'BTH',
    'Blu-ray': 'BRY',
    'Board book': 'BBK',
    'board_book': 'BBK',  # Alternate format
    'Bonded Leather': 'BLE',
    'Calendar': 'CAL',
    'Card Book': 'CRB',
    'Cards': 'CRD',
    'CD-ROM': 'CDR',
    'Diary': 'DIR',
    'DVD': 'DVD',
    'DVD Audio': 'DVA',
    'DVD-ROM': 'DVR',
    'Flexibound': 'FLX',
    'Game': 'GAM',
    'Hardcover': 'HCV',
    'Hardcover-spiral': 'HCS',
    'Imitation Leather': 'IML',
    'JP Oversized': 'JPO',
    'Kindle Edition': 'KIN',
    'Kindle Edition with Audio/Video': 'KAV',
    'Kitchen': 'KIT',
    'Leather Bound': 'LTH',
    'Library Binding': 'LIB',
    'Loose Leaf': 'LSL',
    'Map': 'MAP',
    'Mass Market Paperback': 'MMP',
    'mass_market': 'MMP',  # Alternate format
    'Misc.': 'MSC',
    'Misc. Supplies': 'MSC',
    'Notebook': 'NTB',
    'Novelty Book': 'NOV',
    'Office Product': 'OFP',
    'Pamphlet': 'PAM',
    'Paperback': 'PBK',
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



def generate_book_title(book_name, author, binding_type=None, publication_year=None, binding_codes=None):
    binding_codes = binding_codes or {}
    ellipsis = "…"
    
    # Helper to truncate text with ellipsis at end
    def truncate(text, max_len):
        return text[:max_len-1] + ellipsis if len(text) > max_len else text

    # Determine binding abbreviation if present
    binding_abbr = None
    if binding_type:
        if binding_type == 'Paperback':
            binding_abbr = 'PB'
        elif binding_type == 'Hardcover':
            binding_abbr = 'HC'
        else:
            binding_abbr = binding_codes.get(binding_type, binding_type)

    # Build components dynamically
    components = {
        'book': book_name,
        'by': "by",
        'author': author,
        'binding': binding_abbr,
        'year': publication_year
    }

    # Generate title variations in priority order
    variations = []
    
    # Variation 1: Full format with binding/year
    variations.append(f"{book_name} by {author} {binding_type or ''} {publication_year or ''}".strip())
    
    # Variation 2: Abbreviated binding
    variations.append(f"{book_name} by {author} {binding_abbr or ''} {publication_year or ''}".strip())
    
    # Variation 3: Remove "by"
    variations.append(f"{book_name} {author} {binding_abbr or ''} {publication_year or ''}".strip())
    
    # Variation 4: Truncate author
    base_length = len(f"{book_name} ") + len(f" {binding_abbr or ''} {publication_year or ''}".strip()) + 1
    max_author_len = 80 - base_length - 1  # -1 for ellipsis
    if max_author_len >= 1:
        truncated_author = truncate(author, max_author_len)
        variations.append(f"{book_name} {truncated_author} {binding_abbr or ''} {publication_year or ''}".strip())
    
    # Variation 5: Truncate book name
    base_length = len(f" {author} {binding_abbr or ''} {publication_year or ''}".strip()) + 1
    max_book_len = 80 - base_length - 1  # -1 for ellipsis
    if max_book_len >= 1:
        truncated_book = truncate(book_name, max_book_len)
        variations.append(f"{truncated_book} {author} {binding_abbr or ''} {publication_year or ''}".strip())
    
    # Find first valid variation
    for title in variations:
        if len(title) <= 80:
            return title[:80]  # Ensure exact length
    
    # Final fallback: Book title only with ellipsis
    return truncate(book_name, 77).ljust(80, ellipsis)[:80]

def extract_year(date_str):
    """Extract year from various date formats"""
    match = re.search(r'\b\d{4}\b', str(date_str))
    return match.group(0) if match else None


def calculate_start_price(item):
    """
    Calculate listing price based on RRP, discount, and VAT
    Returns rounded price or None if invalid data
    """
    try:
        # Get required values
        rrp = float(item['rrp'])
        discount_percent = float(item['discount'])
        vat_percent = (item['vat_code']).lower()
        
        if vat_percent == 'z':
            vat_percent = float(0.0)
        elif vat_percent == 's':
            vat_percent = float(20.0)
        else:
            vat_percent = float(5.0)

        # Validate input values
        if any(val < 0 for val in [rrp, discount_percent, vat_percent]):
            print(f"Invalid negative value in pricing data for item {item.get('id')}")
            return None

        # Calculate discounted price
        discounted_price = rrp * (1 - (discount_percent / 100))
        
        # Calculate net price after VAT
        net_price = discounted_price / (1 + (vat_percent / 100))
        
        # Return price rounded to 2 decimal places
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

    # eBay OAuth Flow
    auth_url = f"https://auth.ebay.com/oauth2/authorize?client_id={EBAY_CREDENTIALS['client_id']}&redirect_uri={urllib.parse.quote(EBAY_CREDENTIALS['redirect_uri'])}&response_type=code&scope=https://api.ebay.com/oauth/api_scope/sell.inventory"
    print(f"Authorize here: {auth_url}")
    redirect_url = input("Paste redirect URL after authorization: ")
    
    # Extract authorization code
    code = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_url).query)['code'][0]
    
    # Get access token
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

    # Initialize eBay API connection
    connection = Connection(
        domain='api.ebay.com',
        config_file=None,
        certid=EBAY_CREDENTIALS['client_secret'],
        appid=EBAY_CREDENTIALS['client_id'],
        devid=EBAY_CREDENTIALS['dev_id'],
        token=access_token,
        site_id="3"
    )
    success_count = 0
    # Process inventory
    inventory = supabase.table(table_name).select('*').execute().data
    for item in inventory:
        
        try:
            language = item.get('language', 'en')
            if language == 'en':
                language = 'English'
            

            # Format components
            binding = item.get('binding', 'unknown')
            pub_year = extract_year(item.get('publication_year'))
            title = generate_book_title(
                item['title'],
                item['author'], binding,
                pub_year, BINDING_SHORTCODES
                
            )

            # Build listing payload
            payload = {
                "Item": {
                    "Title": title,
                    "Description": item.get('description', 'No description available'),
                    "PrimaryCategory": {"CategoryID": "29290"},  
                    "ConditionID": str(item.get('condition_id', 3000)),  
                    "Currency": "GBP",
                    "StartPrice": '45',
                    "Quantity": str(item['stock']),
                    "Country": "US",
                    "Location": item.get('location', 'San Francisco, CA'),
                    "ListingDuration": "GTC",
                    "BusinessPolicies": {
                        
                        "PaymentPolicyID": EBAY_CREDENTIALS['business_policies']['payment']
                        
                    },
                    "ReturnPolicy": {
                    "ReturnsAcceptedOption": "ReturnsAccepted",
                    "RefundOption": "MoneyBack",
                    "ReturnsWithinOption": "Days_30",
                    "ShippingCostPaidByOption": "Buyer"
                    },
                    "ShippingDetails": {
                    "ShippingServiceOptions": {
                        "ShippingServicePriority": "1",
                        "ShippingService": "USPSMedia",
                        "ShippingServiceCost": "2.50",
                        "FreeShipping": "false",
                        "ShippingServiceAdditionalCost": "0.00"  # Added to fix shipping warning
                    }
                },
                    "DispatchTimeMax": "1",
                    "ProductListingDetails": {
                        "ISBN": item['isbn13'],
                        "IncludeStockPhotoURL": "true"
                    },
                    "ItemSpecifics": {
                        "NameValueList": [
                            {"Name": "Title", "Value": [title]},
                            {"Name": "Author", "Value": [item['author'].split()[-1]]},
                            {"Name": "Binding", "Value": [binding]},
                            {"Name": "Language", "Value": [language]},
                            {"Name": "ISBN", "Value": [item.get('isbn13', 'Unknown')]},
                            {"Name": "Publisher", "Value": [item.get('publisher', 'Unknown')]}
                        ]
                    }
                },
                "WarningLevel": "High",
                "ErrorLanguage": "en_US",
                  
            }

            # Add publication year if available
            if pub_year:
                payload["Item"]["ItemSpecifics"]["NameValueList"].append(
                    {"Name": "Publication Year", "Value": [pub_year]}
                )

            # Add image if available
            if item.get('cover_image'):
                payload["Item"]["PictureDetails"] = {"PictureURL": [item['cover_image']]}
                
            calculated_price = calculate_start_price(item)
            if not calculated_price or calculated_price < 0.99:
                print(f"Skipping item {item.get('id')} - invalid price calculation")
                continue
            payload["Item"]["StartPrice"] = f"{calculated_price:.2f}"
            
            
            
            

            # Submit listing
            response = connection.execute('AddFixedPriceItem', payload)
            success_count += 1
            
            if response.dict()['Ack'] == 'Warning':
                print(f"Successfully listed: {title} (ID: {response.dict()['ItemID']})")
            else:
                print(f"Error listing {title}: {response.dict().get('Errors', 'Unknown error')}")
            
            time.sleep(1)  # Rate limit

        except Exception as e:
            print(f"Failed to process item {item.get('id')}: {str(e)}")
            continue

if __name__ == "__main__":
    main()