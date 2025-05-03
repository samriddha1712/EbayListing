import os
import base64
import urllib.parse
import requests

# ─── Configuration ────────────────────────────────────────────────────────────
CLIENT_ID        = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET    = os.getenv("EBAY_CLIENT_SECRET")
REDIRECT_URI     = os.getenv("EBAY_REDIRECT_URI")    # Must match your app settings
SCOPES           = ["https://api.ebay.com/oauth/api_scope/sell.inventory"]
API_BASE         = "https://api.ebay.com/sell/inventory/v1"
LOCATION_KEY     = os.getenv("EBAY_LOCATION_KEY", "LondonWh1")
# Fill in your desired address details here:
LOCATION_PAYLOAD = {
    "location": {
        "address": {
            "addressLine1": "10 Downing Street",
            "city":          "London",
            "stateOrProvince":"England",
            "postalCode":    "SW1A 2AA",
            "country":       "GB"
        }
    },
    "locationTypes": ["WAREHOUSE"],
    "name":               "Switzerland Warehouse 1",
    "merchantLocationStatus": "ENABLED"
    
}

# ─── Step 1: Get User Authorization Code ───────────────────────────────────────
def print_authorization_url():
    qs = {
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "response_type": "code",
        "scope":         " ".join(SCOPES),
    }
    url = "https://auth.ebay.com/oauth2/authorize?" + urllib.parse.urlencode(qs)
    print("\n1) Go to this URL in your browser, log in, and grant permissions:\n")
    print(url)
    print("\n2) After approving, you’ll be redirected to your REDIRECT_URI")
    print("   with `?code=…` in the URL. Paste that full redirect URL when prompted.\n")

# ─── Step 2: Exchange Code for User Access Token ──────────────────────────────
def get_user_token(redirected_url: str) -> str:
    code = urllib.parse.parse_qs(urllib.parse.urlparse(redirected_url).query)["code"][0]
    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {auth}"
    }
    data = {
        "grant_type":   "authorization_code",
        "code":         code,
        "redirect_uri": REDIRECT_URI
    }
    resp = requests.post("https://api.ebay.com/identity/v1/oauth2/token",
                         headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

# ─── Step 3: Create Inventory Location ────────────────────────────────────────
def create_location(token: str):
    url     = f"{API_BASE}/location/{LOCATION_KEY}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }
    resp = requests.post(url, headers=headers, json=LOCATION_PAYLOAD)
    if resp.status_code == 204:
        print(f"✅ Location `{LOCATION_KEY}` created.")
    else:
        print(f"❌ Failed to create (HTTP {resp.status_code}):\n{resp.text}")
        resp.raise_for_status()

# ─── Step 4: Retrieve & Confirm Location ──────────────────────────────────────
def get_location(token: str):
    url     = f"{API_BASE}/location/{LOCATION_KEY}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    print("\n🔍 Retrieved location data:\n")
    from pprint import pprint
    pprint(data)

# ─── Main Flow ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print_authorization_url()
    redirect_url = input("Paste full redirect URL here: ").strip()
    token = get_user_token(redirect_url)
    create_location(token)
    get_location(token)
