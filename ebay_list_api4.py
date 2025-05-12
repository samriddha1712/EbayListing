import requests
import urllib.parse
import base64
import re
import time
from supabase import create_client, Client
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring
import os
from calculate_price import inclusive_price

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
        'return_policy_id': '245006369024',
        'payment_policy_id': os.getenv('EBAY_PAYMENT_POLICY_ID'),
        'shipping_policy_id': '245006370024'
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
    'mass_market': 'MMP',  # Alternate format
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
    variations.append(f"{book_name} by {author} {binding_type or ''} Book {publication_year or ''}".strip())
    
    # Variation 2: Abbreviated binding
    variations.append(f"{book_name} by {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    # Variation 3: Remove "by"
    variations.append(f"{book_name} {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    # Variation 4: Truncate author
    base_length = len(f"{book_name} ") + len(f" {binding_abbr or ''} Book {publication_year or ''}".strip()) + 1
    max_author_len = 65 - base_length - 1  # -1 for ellipsis
    if max_author_len >= 1:
        truncated_author = truncate(author, max_author_len)
        variations.append(f"{book_name} {truncated_author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    # Variation 5: Truncate book name
    base_length = len(f" {author} {binding_abbr or ''} Book {publication_year or ''}".strip()) + 1
    max_book_len = 65 - base_length - 1  # -1 for ellipsis
    if max_book_len >= 1:
        truncated_book = truncate(book_name, max_book_len)
        variations.append(f"{truncated_book} {author} {binding_abbr or ''} Book {publication_year or ''}".strip())
    
    # Find first valid variation
    for title in variations:
        if len(title) <= 65:
            return title[:65]  # Ensure exact length
    
    # Final fallback: Book title only with ellipsis
    return truncate(book_name, 62).ljust(65, ellipsis)[:65]

def extract_year(date_str):
    """Extract year from various date formats"""
    match = re.search(r'\b\d{4}\b', str(date_str))
    return match.group(0) if match else None


def calculate_start_price(item , final_price):
    """
    Calculate listing price based on RRP, discount, and VAT
    Returns rounded price or None if invalid data
    """
    try:
                
        vat_percent = (item['vat_code']).lower()
        
        if vat_percent == 'z':
            vat_percent = float(0.0)
        elif vat_percent == 's':
            vat_percent = float(20.0)
        else:
            vat_percent = float(5.0)

        # Validate input values
        if any(val < 0 for val in [vat_percent]):
            print(f"Invalid negative value in pricing data for item {item.get('id')}")
            return None

        net_price = final_price * (1 + (vat_percent / 100))
        
        # Return price rounded to 2 decimal places
        return round(net_price, 2)
    
    except KeyError as e:
        print(f"Missing required pricing field {e} for item {item.get('id')}")
        return None
    except (TypeError, ValueError) as e:
        print(f"Invalid pricing data for item {item.get('id')}: {str(e)}")
        return None

def build_ebay_xml(item_data, access_token, calculated_price, title, binding_type, pub_year):
    """Build XML request matching the example structure"""
    root = Element('VerifyAddItemRequest', xmlns='urn:ebay:apis:eBLBaseComponents')
    
    # Requester Credentials
    requester = SubElement(root, 'RequesterCredentials')
    SubElement(requester, 'eBayAuthToken').text = access_token
    
    # Item container
    item = SubElement(root, 'Item')
    
    # Title and Description
    SubElement(item, 'Title').text = title
    SubElement(item, 'Description').text = item_data.get('description', 'No description available')
    
    # Pictures
    if item_data.get('cover_image'):
        pic_details = SubElement(item, 'PictureDetails')
        SubElement(pic_details, 'PictureURL').text = item_data['cover_image']
    
    # Item Specifics
    specifics = SubElement(item, 'ItemSpecifics')
    spec_data = [
        ('Sport', ['Baseball']),  # Hardcoded per example
        ('Condition', ['Ungraded']),
        ('Publication Year', [pub_year]) if pub_year else None,
        ('Author', [item_data['author']]),
        ('Publisher', [item_data.get('publisher', 'Unknown')])
    ]
    
    for entry in filter(None, spec_data):
        nv_list = SubElement(specifics, 'NameValueList')
        SubElement(nv_list, 'Name').text = entry[0]
        SubElement(nv_list, 'Value').text = entry[1][0]

    # Condition Descriptors (from example)
    cond_desc = SubElement(item, 'ConditionDescriptors')
    descriptor = SubElement(cond_desc, 'ConditionDescriptor')
    SubElement(descriptor, 'Name').text = '40001'  # Condition type ID
    SubElement(descriptor, 'Value').text = 'Like New Or Better'  # Condition description

    # Category Information (baseball cards category from example)
    primary_cat = SubElement(item, 'PrimaryCategory')
    SubElement(primary_cat, 'CategoryID').text = '29290'
    SubElement(primary_cat, 'CategoryName').text = 'Books'

    # Pricing and Quantity
    SubElement(item, 'StartPrice').text = f"{calculated_price:.2f}"
    SubElement(item, 'CategoryMappingAllowed').text = 'true'
    SubElement(item, 'Quantity').text = str(item_data['stock'])

    # Shipping Configuration (from example)
    shipping = SubElement(item, 'ShippingDetails')
    SubElement(shipping, 'ShippingDiscountProfileID').text = '0'
    SubElement(shipping, 'InternationalShippingDiscountProfileID').text = '0'
    
    pkg_details = SubElement(shipping, 'ShippingPackageDetails')
    SubElement(pkg_details, 'MeasurementUnit').text = 'English'
    SubElement(pkg_details, 'PackageDepth', {'unit':'in', 'measurementSystem':'English'}).text = '1'
    SubElement(pkg_details, 'PackageLength', {'unit':'in', 'measurementSystem':'English'}).text = '1'
    SubElement(pkg_details, 'PackageWidth', {'unit':'in', 'measurementSystem':'English'}).text = '1'
    SubElement(pkg_details, 'ShippingIrregular').text = 'false'
    SubElement(pkg_details, 'ShippingPackage').text = 'PackageThickEnvelope'
    SubElement(pkg_details, 'WeightMajor', {'unit':'lbs'}).text = '0'
    SubElement(pkg_details, 'WeightMinor', {'unit':'oz'}).text = '1'

    # Seller Profiles (from example structure)
    seller_profiles = SubElement(item, 'SellerProfiles')
    
    payment_profile = SubElement(seller_profiles, 'SellerPaymentProfile')
    SubElement(payment_profile, 'PaymentProfileID').text = EBAY_CREDENTIALS['business_policies']['payment_policy_id']
    
    return_profile = SubElement(seller_profiles, 'SellerReturnProfile')
    SubElement(return_profile, 'ReturnProfileID').text = EBAY_CREDENTIALS['business_policies']['return_policy_id']
    
    shipping_profile = SubElement(seller_profiles, 'SellerShippingProfile')
    SubElement(shipping_profile, 'ShippingProfileID').text = EBAY_CREDENTIALS['business_policies']['shipping_policy_id']

    # Required Fields from Example
    SubElement(item, 'ConditionID').text = '4000'  # Ungraded condition code
    SubElement(item, 'ConditionDisplayName').text = 'Ungraded'
    SubElement(item, 'Country').text = 'GB'
    SubElement(item, 'Currency').text = 'GBP'
    SubElement(item, 'DispatchTimeMax').text = '3'
    SubElement(item, 'ListingDuration').text = 'GTC'
    SubElement(item, 'ListingType').text = 'FixedPriceItem'
    SubElement(item, 'PostalCode').text = 'PA145YU'  # Replace with your postal code
    SubElement(item, 'Site').text = 'UK'

    return ET.tostring(root, encoding='utf-8', method='xml')

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # eBay OAuth Flow
    auth_url = f"https://auth.ebay.com/oauth2/authorize?client_id={EBAY_CREDENTIALS['client_id']}&redirect_uri={urllib.parse.quote(EBAY_CREDENTIALS['redirect_uri'])}&response_type=code&scope=https://api.ebay.com/oauth/api_scope/sell.inventory"
    print(f"Authorize here: {auth_url}")
    redirect_url = input("Paste redirect URL after authorization: ")
    code = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_url).query)['code'][0]

    # Get access token
    token_response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64.b64encode(
                f"{EBAY_CREDENTIALS['client_id']}:{EBAY_CREDENTIALS['client_secret']}".encode()
            ).decode()
        },
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": EBAY_CREDENTIALS['redirect_uri']
        }
    )
    access_token = token_response.json()['access_token']

    success_count = 0
    inventory = supabase.table(table_name).select('*').execute().data

    for item in inventory:
        try:
            # Generate listing details
            binding = item.get('binding', 'Unknown')
            pub_year = extract_year(item.get('publication_year'))
            title = generate_book_title(
                item['title'],
                item['author'],
                binding,
                pub_year,
                BINDING_SHORTCODES
            )

            # Calculate price
            calculated_price_vat_excl = inclusive_price(item)
            calculated_price = calculate_start_price(item, calculated_price_vat_excl)
            
            if not calculated_price or calculated_price < 0.99:
                print(f"Skipping item {item.get('id')} - invalid price")
                continue

            # Build XML request
            xml_data = build_ebay_xml(
                item_data=item,
                access_token=access_token,
                calculated_price=calculated_price,
                title=title,
                binding_type=binding,
                pub_year=pub_year
            )

            print(xml_data)

            # Send request to eBay
            headers = {
                'X-EBAY-API-COMPATIBILITY-LEVEL': '1245',
                'X-EBAY-API-CALL-NAME': 'VerifyAddItem',
                'X-EBAY-API-SITEID': '3',
                'Content-Type': 'text/xml'
            }
            
            response = requests.post(
                'https://api.ebay.com/ws/api.dll',
                data=xml_data,
                headers=headers
            )

            # Process response
            if response.status_code == 200:
                response_root = ET.fromstring(response.content)
                ack = response_root.find('Ack').text
                if ack in ['Success', 'Warning']:
                    item_id = response_root.find('ItemID').text
                    print(f"Successfully listed: {title} (ID: {item_id})")
                    success_count += 1
                else:
                    errors = response_root.findall('Errors')
                    for error in errors:
                        print(f"Error: {error.find('LongMessage').text}")
            else:
                print(f"API Error: {response.status_code} - {response.text}")

            time.sleep(1)  # Rate limiting

        except Exception as e:
            print(f"Failed to process item {item.get('id')}: {str(e)}")
            continue

    print(f"Successfully listed {success_count} items")

if __name__ == "__main__":
    main()