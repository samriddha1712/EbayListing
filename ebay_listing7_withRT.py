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


def generate_book_title(book_name, author, binding_type=None, publication_year=None, binding_codes=None, max_len=65):
    binding_codes = binding_codes or {}
    default_map = {'Paperback': 'Pb', 'Hardcover': 'Hc'}
    code_map = {**default_map, **binding_codes}

    parts = [book_name, 'by', author]
    if binding_type:
        parts += [binding_type, 'Book']
    if publication_year:
        parts.append(str(publication_year))
    title = ' '.join(parts)
    title = re.sub(r'\s+', ' ', title).strip()
    if len(title) <= max_len:
        return title

    if publication_year:
        title = re.sub(r'\s+' + re.escape(str(publication_year)) + r'$', '', title).strip()
        if len(title) <= max_len:
            return title

    if binding_type and binding_type in code_map:
        title = re.sub(r'\b' + re.escape(binding_type) + r'\b', code_map[binding_type], title).strip()
        if len(title) <= max_len:
            return title

    title = re.sub(r'\bby\b', '', title).strip()
    if len(title) <= max_len:
        return title

    names = author.split()
    if len(names) > 1:
        abbr_author = names[0][0] + '.' + ' '.join(names[1:])
    else:
        abbr_author = names[0][0] + '.'
    title = title.replace(author, abbr_author).strip()
    if len(title) <= max_len:
        return title

    return title[:max_len-3].rstrip() + '...'


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



def main():
    
    scopes = "https://api.ebay.com/oauth/api_scope/sell.inventory" \
         "https://api.ebay.com/oauth/api_scope/offline_access"
         
    encoded_scopes = urllib.parse.quote(scopes, safe='')
    
    
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    auth_url = (
        f"https://auth.ebay.com/oauth2/authorize?client_id={EBAY_CREDENTIALS['client_id']}"
        f"&redirect_uri={urllib.parse.quote(EBAY_CREDENTIALS['redirect_uri'])}&response_type=code"
        f"&scope=https://api.ebay.com/oauth/api_scope/sell.inventory"
    )
    print(f"Authorize here: {auth_url}")
    redirect_url = input("Paste redirect URL after authorization: ")
    code = urllib.parse.parse_qs(urllib.parse.urlparse(redirect_url).query)['code'][0]

    token_response = requests.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + base64.b64encode(
                f"{EBAY_CREDENTIALS['client_id']}:{EBAY_CREDENTIALS['client_secret']}".encode()
            ).decode()
        },
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": EBAY_CREDENTIALS['redirect_uri']}
    )
    access_token = token_response.json().get('access_token')

    connection = Connection(
        debug=False,config_file=None, domain='api.ebay.com', certid=EBAY_CREDENTIALS['client_secret'],
        appid=EBAY_CREDENTIALS['client_id'], devid=EBAY_CREDENTIALS['dev_id'], token=access_token, siteid=3
    )

    success_count = 0
    inventory = supabase.table(table_name).select('*').order('publication_year', desc=True).execute().data

    for item in inventory:
        
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
                    "PostalCode": xml_safe("PA145YU"),
                    "ItemSpecifics": {
                        "NameValueList": [
                            {"Name": "Title", "Value": [safe_title]},
                            {"Name": "Author", "Value": [author_val]},
                            {"Name": "Binding", "Value": [binding_val]},
                            {"Name": "Language", "Value": [xml_safe(language)]},
                            {"Name": "ISBN", "Value": [isbn_val]},
                            {"Name": "Publisher", "Value": [publisher_val]}
                        ]
                    },
                    "BusinessPolicies": {"PaymentPolicyID": EBAY_CREDENTIALS['business_policies']['payment']},
                    "ReturnPolicy": {"ReturnsAcceptedOption": "ReturnsAccepted", "RefundOption": "MoneyBack", "ReturnsWithinOption": "Days_30", "ShippingCostPaidByOption": "Buyer"},
                    "ShippingDetails": {"ShippingServiceOptions": [{"ShippingServicePriority": "1", "ShippingService": "UK_RoyalMailSecondClassStandard", "ShippingServiceCost": "3.00", "FreeShipping": "false", "ShippingServiceAdditionalCost": "0.00"},{'ShippingServicePriority': '2', 'ShippingService': 'UK_RoyalMail24', 'ShippingServiceCost': '2.95', 'FreeShipping': 'false', 'ShippingServiceAdditionalCost': '2.95'}]},
                    
                    # 'ShippingDetails': {'ShippingType': 'Flat', 'ShippingServiceOptions': [{'ShippingServicePriority': '1', 'ShippingService': 'UK_RoyalMailTracked48', 'ShippingServiceCost': '0.00', 'FreeShipping': 'true', 'ShippingServiceAdditionalCost': '0.00'}, {'ShippingServicePriority': '2', 'ShippingService': 'UK_RoyalMail24', 'ShippingServiceCost': '2.95', 'FreeShipping': 'false', 'ShippingServiceAdditionalCost': '2.95'}]},
                  
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
            if response.dict().get('Ack') == 'Warning':
                print(f"Successfully listed: {safe_title} (ID: {response.dict()['ItemID']})")
                success_count += 1
                print("Total successful listings:", success_count)
        except Exception as e:
            print(f"{payload}\n\n")
            print(f"Failed item {item.get('id')}: {e}")


if __name__ == "__main__":
    main()
