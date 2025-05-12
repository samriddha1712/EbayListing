import requests
import urllib.parse
import base64
import re
import time
from supabase import create_client, Client
from ebaysdk.trading import Connection
from ebaysdk.exception import ConnectionError
from ebaysdk.utils import dict2xml
from xml.sax.saxutils import escape
import os
from calculate_price import inclusive_price
from typing import List

# Configuration
RUN_SCRIPT = os.getenv('RUN_SCRIPT')
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
refresh_token = os.getenv('REFRESH_TOKEN')

TOKEN_LIFESPAN = 1.55 * 3600

access_token = None
token_obtained_ts = 0.0

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
}  # (unchanged for brevity)


def generate_book_title(
    book_name: str = '',
    author: str = '',
    binding_type: str = None,
    publication_year: str = None,
    binding_codes: dict = None,
    max_len: int = 65
) -> str:
    """
    Construct a concise title from available info. Missing fields are skipped gracefully.
    """
    binding_codes = binding_codes or {}
    default_map = {'Paperback': 'Pb', 'Hardcover': 'Hc'}
    code_map = {**default_map, **binding_codes}

    parts: List[str] = []
    # Add book name
    if book_name:
        parts.append(book_name)
    # Add author
    if author:
        parts.extend(['by', author])
    # Add binding type
    if binding_type:
        parts.extend([binding_type, 'Book'])
    # Add publication year
    if publication_year:
        parts.append(str(publication_year))

    # Join and normalize whitespace
    title = ' '.join(parts)
    title = re.sub(r'\s+', ' ', title).strip()

    # If within limits, return early
    if len(title) <= max_len:
        return title

    # Remove publication year if too long
    if publication_year and title.endswith(str(publication_year)):
        title = title[:-(len(str(publication_year))+1)].strip()
        if len(title) <= max_len:
            return title

    # Replace binding_type with shortcode
    if binding_type and binding_type in code_map:
        title = re.sub(
            rf'\b{re.escape(binding_type)}\b',
            code_map[binding_type],
            title
        ).strip()
        if len(title) <= max_len:
            return title

    # Remove 'by' to shorten
    title_no_by = re.sub(r'\bby\b', '', title).strip()
    if len(title_no_by) <= max_len:
        return title_no_by

    # Abbreviate author
    if author:
        names = author.split()
        if len(names) > 1:
            abbr_author = names[0][0] + '. ' + ' '.join(names[1:])
        else:
            abbr_author = names[0][0] + '.'
        title = title.replace(author, abbr_author).strip()
        if len(title) <= max_len:
            return title

    # Fallback to hard truncate
    return title[:max_len-3].rstrip() + '...'


def refresh_access_token():
    global access_token, token_obtained_ts, connection
    resp = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64.b64encode(
                f"{EBAY_CREDENTIALS['client_id']}:{EBAY_CREDENTIALS['client_secret']}".encode()
            ).decode()
        },
        data={
            "grant_type":     "refresh_token",
            "refresh_token":  refresh_token,
            "scope":          "https://api.ebay.com/oauth/api_scope/sell.inventory"
        }
    )
    resp.raise_for_status()
    data = resp.json()
    access_token = data["access_token"]
    token_obtained_ts = time.time()

    # update the Connection object if it already exists
    if 'connection' in globals():
        connection.token = access_token
        
        
def ensure_token_valid():
    if access_token is None or (time.time() - token_obtained_ts) > TOKEN_LIFESPAN:
        refresh_access_token()
    return access_token


def extract_year(date_str):
    match = re.search(r'\b\d{4}\b', str(date_str))
    return match.group(0) if match else None


def calculate_start_price(item, final_price):
    try:
        vat_code = item.get('vat_code', 's').lower()
        vat_percent = {'z': 0.0, 's': 20.0}.get(vat_code, 5.0)
        if vat_percent < 0:
            print(f"Invalid VAT for item {item.get('id')}")
            return None
        net_price = final_price * (1 + vat_percent/100)
        return round(net_price, 2)
    except Exception as e:
        print(f"Price calc error for {item.get('id')}: {e}")
        return None


def xml_safe(text):
    return escape(text, {"\"": "&quot;", "'": "&apos;"})


def sanitize_description(raw_description):
    # Remove HTML <a> tags completely
    no_html_links = re.sub(r'<a\s+[^>]href=[\'"][^\'"][\'"][^>]>(.?)</a>', r'\1 [LINK]', raw_description, flags=re.IGNORECASE)
    
    # Replace plain URLs (http, https, www) with [LINK]
    no_plain_links = re.sub(r'https?://\S+|www\.\S+', '[LINK]', no_html_links, flags=re.IGNORECASE)
    
    return no_plain_links


def stock_visiblity(actual_stock):
    if actual_stock > 50:
        visible_quantity = 10
    elif actual_stock < 10:
        visible_quantity = 2
    else:
        visible_quantity = actual_stock
        
    return visible_quantity


def get_existing_columns(supabase: Client, table_name: str) -> List[str]:
    """Retrieve current table columns from Supabase using a row sample"""
    try:
        result = supabase.table(table_name).select("*").limit(1).execute()
        if result.data and len(result.data) > 0:
            return list(result.data[0].keys())
        elif result.data == []:
            # No rows, but table exists: get from 'columns' attribute
            # Some Supabase clients return column metadata in response
            return result.model_dump().get("columns", [])
        return []
    except Exception as e:
        print(f"❌ Column detection error: {e}")
        return []


def main():

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Connected to Supabase")
    
    columns = get_existing_columns(supabase, table_name)
    print(f"🧾 Columns in {table_name}:", columns)

    if "listed" in columns:
        print("ℹ️ 'listed' column already exists.")
    else:
        print("➕ 'listed' column missing — adding it...")  
    
        sql = f"""
        ALTER TABLE {table_name}
        ADD COLUMN IF NOT EXISTS listed boolean
            DEFAULT FALSE NOT NULL;
        """
        try:
        # Execute via RPC
            res = supabase.rpc('execute_sql', {'sql': sql}).execute()
            
            code = getattr(res, 'status_code', None)
            if code is not None and code >= 400:
                print(f"❌ Failed to add 'listed' column ({code}):", res.data)
            else:
                print("✅ 'listed' column is ensured (default FALSE).")
        except Exception as e:
        # catch network/validation errors
            print("❌ RPC call failed:", e)

        
    
    
    
    refresh_access_token()
    global connection

    connection = Connection(
        debug=False,config_file=None, domain='api.ebay.com', certid=EBAY_CREDENTIALS['client_secret'],
        appid=EBAY_CREDENTIALS['client_id'], devid=EBAY_CREDENTIALS['dev_id'], token=access_token, siteid=3
    )

    success_count = 0
    inventory = supabase.table(table_name).select('*').order('publication_year', desc=False).eq('listed', False).execute().data

    for item in inventory:
                
        ensure_token_valid()
        
        try:
            language = 'English' if item.get('language', 'en') == 'en' else item.get('language')
            pub_year = extract_year(item.get('publication_year'))
            raw_title = generate_book_title(item['title'], item['author'], item.get('binding'), pub_year, BINDING_SHORTCODES)
            safe_title = xml_safe(raw_title)

            # Escape item specifics values
            author_val = xml_safe(item['author'].split()[-1])
            binding_val = xml_safe(item.get('binding', 'Unknown'))
            isbn_val = xml_safe(item.get('isbn13', 'Unknown'))
            publisher_val = xml_safe(item.get('publisher', 'Unknown'))
            
            description = sanitize_description(item.get('description', 'No description available'))
            
            stock_visible = stock_visiblity(int(item['stock']))
            
            

            payload = {
                "Item": {
                    "Title": safe_title,
                    "Description": f"<![CDATA[{description}]]>",
                    "PrimaryCategory": {"CategoryID": "261186"},
                    "StartPrice": "9.99",
                    "CategoryMappingAllowed": "true",
                    "Country": "GB",
                    "Currency": "GBP",
                    "ConditionID": "1000",
                    "DispatchTimeMax": "1",
                    "ListingDuration": "GTC",
                    "ListingType": "FixedPriceItem",
                    "Quantity": str(stock_visible),
                    "Location": xml_safe("Port Glasgow"),
                    "PostalCode": xml_safe("PA14 5YU"),
                    "ItemSpecifics": {
                        "NameValueList": [
                            {"Name": "Title", "Value": [safe_title]},
                            {"Name": "Author", "Value": [author_val]},
                            {"Name": "Binding", "Value": [binding_val]},
                            {"Name": "Language", "Value": [xml_safe(language)]},
                            {"Name": "ISBN", "Value": [isbn_val]},
                            {"Name": "Publisher", "Value": [publisher_val]},
                            {"Name": "Topic", "Value": "Books"},
                            {"Name": "Format", "Value": [binding_val]}
                        ]
                    },
                    "BusinessPolicies": {"PaymentPolicyID": EBAY_CREDENTIALS['business_policies']['payment']},
                    "ReturnPolicy": {"ReturnsAcceptedOption": "ReturnsAccepted", "RefundOption": "MoneyBack", "ReturnsWithinOption": "Days_30", "ShippingCostPaidByOption": "Buyer"},
                    
                    
                    # 'ShippingDetails': {'ShippingServiceOptions':[{'ShippingServicePriority':1,'ShippingService':'UK_RoyalMailTracked48','FreeShipping':True,'ShippingServiceCost':{'value':'0.00','currencyID':'GBP'},'ShippingServiceAdditionalCost':{'value':'0.00','currencyID':'GBP'}},{'ShippingServicePriority':2,'ShippingService':'UK_RoyalMail24','FreeShipping':False,'ShippingServiceCost':{'value':'2.95','currencyID':'GBP'},'ShippingServiceAdditionalCost':{'value':'2.95','currencyID':'GBP'}}]},

                    # "ShippingDetails":{"ShippingType":"Flat","ShippingServiceOptions":[{"ShippingServicePriority":1,"ShippingService":"UK_RoyalMailTracked","FreeShipping":"true","ShippingServiceCost":{"@currencyID":"GBP","__value__":"0.0"},"ShippingServiceAdditionalCost":{"@currencyID":"GBP","__value__":"2.95"}},{"ShippingServicePriority":2,"ShippingService":"UK_RoyalMail24","FreeShipping":"false","ShippingServiceCost":{"@currencyID":"GBP","__value__":"2.95"}, "ShippingServiceAdditionalCost":{"@currencyID":"GBP","__value__":"2.95"}}]},
                    
                    
                    "ShippingDetails":{"ShippingType":"Flat","ShippingServiceOptions":[{"ShippingServicePriority":1,"ShippingService":"UK_RoyalMailTracked","FreeShipping":"true","ShippingServiceCost":"0.00","ShippingServiceAdditionalCost":"0.00"},{"ShippingServicePriority":2,"ShippingService":"UK_RoyalMailNextDay","FreeShipping":"false","ShippingServiceCost":"2.95", "ShippingServiceAdditionalCost":"2.95"}]},


                    
                    
                    # "ShippingDetails": {"ShippingServiceOptions": [{"ShippingServicePriority": "1", "ShippingService": "UK_RoyalMailSecondClassStandard", "ShippingServiceCost": "3.00", "FreeShipping": "false", "ShippingServiceAdditionalCost": "0.00"},{'ShippingServicePriority': '2', 'ShippingService': 'UK_RoyalMail24', 'ShippingServiceCost': '2.95', 'FreeShipping': 'false', 'ShippingServiceAdditionalCost': '2.95'}]},
                    
                    # 'ShippingDetails': {'ShippingServiceOptions': [{'ShippingServicePriority': '1', 'ShippingService': 'UK_RoyalMailTracked48', 'ShippingServiceCost': '0.00', 'FreeShipping': 'true', 'ShippingServiceAdditionalCost': '0.00'}, {'ShippingServicePriority': '2', 'ShippingService': 'UK_RoyalMail24', 'ShippingServiceCost': '2.95', 'FreeShipping': 'false', 'ShippingServiceAdditionalCost': '2.95'}]},
                  
                    "ProductListingDetails": {"ISBN": isbn_val}
                }
            }

            if pub_year:
                payload["Item"]["ItemSpecifics"]["NameValueList"].append({"Name": "Publication Year", "Value": [xml_safe(pub_year)]})
            if item.get('cover_image'):
                payload["Item"]["PictureDetails"] = {"PictureURL": [item['cover_image']]}  # URLs are safe

            vat_excl = inclusive_price(item)
            start_price = calculate_start_price(item, vat_excl)
            if not start_price or start_price < 0.99:
                continue
            payload["Item"]["StartPrice"] = f"{start_price:.2f}"

            response = connection.execute('AddFixedPriceItem', payload)
            if response.dict().get('Ack') in ('Success','Warning'):
                supabase.table(table_name).update({'listed': True}).eq('id', item['id']).execute()
                print(f"Successfully listed: {safe_title} (ID: {response.dict()['ItemID']})")
                success_count += 1
                print("Total successful listings:", success_count)
        except ConnectionError as e:
            # if it’s a token-expired error, refresh+retry once
            if 'Invalid token' in str(e) or 'token expired' in str(e).lower():
                print("Access token expired mid-run, refreshing…")
                refresh_access_token()
                response = connection.execute('AddFixedPriceItem', payload)
                if response.dict().get('Ack') in ('Success','Warning'):
                    supabase.table(table_name).update({'listed': True})\
                        .eq('id', item['id']).execute()
                    print(f"✅ Listed after refresh: {item['id']}")
            else:
                # some other eBay error
                print(f"eBay error for {item['id']}: {e}")
        except Exception as e:
            print(f"Error processing {item['id']}: {e}")


if __name__ == "__main__":
    if RUN_SCRIPT.lower() == "yes":
        main()
    else:
        print("Script execution skipped because RUN_SCRIPT is not 'yes'.")