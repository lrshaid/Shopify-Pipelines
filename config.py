import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Shopify API Configuration
SHOPIFY_STORE = os.getenv('SHOPIFY_STORE', 'your-store.myshopify.com')
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN', 'your-access-token')

# API Configuration
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-07')
SHOPIFY_API_ENDPOINT = os.getenv('SHOPIFY_API_ENDPOINT', 'admin/api')

# Optional: Additional configuration
DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

# Construct the full API URL
SHOPIFY_API_URL = f"https://{SHOPIFY_STORE}/{SHOPIFY_API_ENDPOINT}/{SHOPIFY_API_VERSION}/graphql.json"

# Headers for API requests
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
} 