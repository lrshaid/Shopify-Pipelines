import requests
import json
import time
from config import SHOPIFY_API_URL, SHOPIFY_HEADERS, SAVE_DATA

def create_bulk_operation(graphql_query: str):
    """Create a bulk operation to execute the provided GraphQL query document."""
    bulk_mutation = f'''
      mutation {{
        bulkOperationRunQuery(
          query: """
      {graphql_query}
      """
        ) {{
          bulkOperation {{
            id
            status
          }}
          userErrors {{
            field
            message
          }}
        }}
      }}
      '''
    response = requests.post(
        SHOPIFY_API_URL,
        headers=SHOPIFY_HEADERS,
        json={"query": bulk_mutation},
        timeout=60,
    )
    return response.json()

def check_bulk_operation_status(operation_id: str):
    """Check the status of a bulk operation by ID."""
    status_query = f'''
      {{
        node(id: "{operation_id}") {{
          ... on BulkOperation {{
            id
            status
            errorCode
            createdAt
            completedAt
            objectCount
            fileSize
            url
          }}
        }}
      }}
      '''
    response = requests.post(
        SHOPIFY_API_URL,
        headers=SHOPIFY_HEADERS,
        json={"query": status_query},
        timeout=60,
    )
    return response.json()

def download_bulk_data(url: str) -> str:
    """Download the bulk operation results from the signed URL."""
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    return response.text

# Bulk query to fetch orders with line items
bulk_query = """
    {
      orders {
        edges {
          node {
            id
            name
            createdAt
            processedAt
            currencyCode
            totalPriceSet { shopMoney { amount currencyCode } }
            tags
            note
            customer { id firstName lastName email }
            shippingAddress { name address1 address2 city province country zip phone }
            billingAddress { name address1 address2 city province country zip phone }
            lineItems {
              edges {
                node {
                  id
                  title
                  quantity
                  fulfillableQuantity
                  fulfillmentStatus
                  variant { id sku title }
                  originalUnitPriceSet { shopMoney { amount currencyCode } }
                  discountedTotalSet { shopMoney { amount currencyCode } }
                }
              }
            }
          }
        }
      }
    }
    """

print("Creating bulk operation...")
create_result = create_bulk_operation(bulk_query)

# Basic shape/transport error checks
if not isinstance(create_result, dict):
    print("Error: Unexpected response type when creating bulk op:", type(create_result))
    exit(1)

if "errors" in create_result and create_result["errors"]:
    print("Error creating bulk operation (top-level errors):")
    print(json.dumps(create_result, indent=2))
    exit(1)

data_block = create_result.get("data")
if not data_block:
    print("Error: Response did not include 'data' when creating bulk op.")
    print(json.dumps(create_result, indent=2))
    exit(1)

bulk_run = data_block.get("bulkOperationRunQuery")
if not bulk_run:
    print("Error: 'data.bulkOperationRunQuery' missing or null.")
    print(json.dumps(create_result, indent=2))
    exit(1)

user_errors = bulk_run.get("userErrors") or []
if user_errors:
    print("User errors returned when creating bulk operation:")
    print(json.dumps(user_errors, indent=2))
    exit(1)

bulk_operation = bulk_run.get("bulkOperation")
if not bulk_operation:
    print("Error: 'data.bulkOperationRunQuery.bulkOperation' missing or null.")
    print(json.dumps(create_result, indent=2))
    exit(1)

operation_id = bulk_operation.get("id")
status = bulk_operation.get("status")
if not operation_id or not status:
    print("Error: Bulk operation 'id' or 'status' missing.")
    print(json.dumps(create_result, indent=2))
    exit(1)

print(f"Bulk operation created with ID: {operation_id}")
print(f"Initial status: {status}")

# Poll for completion
terminal_statuses = {"COMPLETED", "FAILED", "CANCELED"}
while status not in terminal_statuses:
    print(f"Operation status: {status}")
    time.sleep(5)

    status_result = check_bulk_operation_status(operation_id)

    if not isinstance(status_result, dict):
        print("Error: Unexpected response type when checking status:", type(status_result))
        exit(1)

    if "errors" in status_result and status_result["errors"]:
        print("Error checking bulk operation status (top-level errors):")
        print(json.dumps(status_result, indent=2))
        exit(1)

    status_data = status_result.get("data")
    if not status_data:
        print("Error: No 'data' in status check response.")
        print(json.dumps(status_result, indent=2))
        exit(1)

    node_data = status_data.get("node")
    if not node_data:
        print("Error: 'data.node' missing or null in status check response.")
        print(json.dumps(status_result, indent=2))
        exit(1)

    status = node_data.get("status")
    if not status:
        print("Error: 'status' missing in node data.")
        print(json.dumps(status_result, indent=2))
        exit(1)

if status == "COMPLETED":
    print("Bulk operation completed!")
    print(f"Objects processed: {node_data.get('objectCount', 'N/A')}")
    print(f"File size: {node_data.get('fileSize', 'N/A')} bytes")

    signed_url = node_data.get("url")
    if not signed_url:
        print("No results URL was returned despite COMPLETED status.")
        exit(1)

    print("Downloading results...")
    data_text = download_bulk_data(signed_url)

    if SAVE_DATA:
      # Save to file (uncomment if you want to persist locally)
      with open('bulk_orders_data.jsonl', 'w') as f:
          f.write(data_text)
      print("Results saved to bulk_orders_data.jsonl")

    # Example: show count of lines
    line_count = len([ln for ln in data_text.splitlines() if ln.strip()])
    print(f"Downloaded {line_count} JSONL lines.")

elif status == "FAILED":
    print("Bulk operation failed!")
    print(f"Error code: {node_data.get('errorCode', 'N/A')}")
    exit(1)
else:
    # CANCELED or other terminal state
    print(f"Bulk operation ended with terminal status: {status}")
    exit(1)