import urllib.parse
import requests
import base64
import os
from pathlib import Path
from dotenv import load_dotenv, set_key

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

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


auth_url = (
        f"https://auth.ebay.com/oauth2/authorize?client_id={EBAY_CREDENTIALS['client_id']}"
        f"&redirect_uri={urllib.parse.quote(EBAY_CREDENTIALS['redirect_uri'])}&response_type=code"
        "&scope=https://api.ebay.com/oauth/api_scope/sell.inventory"
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

refresh_token = token_response.json().get('refresh_token')

set_key(str(env_path), 'REFRESH_TOKEN', refresh_token)
print("✅ REFRESH_TOKEN updated in .env. Safely start your Ebay listing process.")