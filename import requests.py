import requests
import json
import time
from config import SHOPIFY_API_URL, SHOPIFY_HEADERS

def create_bulk_operation(query):
    """Create a bulk operation to execute the query"""
    bulk_mutation = """
    mutation {
      bulkOperationRunQuery(
        query: """
    bulk_mutation += f'"""{query}"""'
    bulk_mutation += """
      ) {
        bulkOperation {
          id
          status
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    response = requests.post(SHOPIFY_API_URL, headers=SHOPIFY_HEADERS, json={"query": bulk_mutation})
    return response.json()

def check_bulk_operation_status(operation_id):
    """Check the status of a bulk operation"""
    query = """
    {
      node(id: """
    query += f'"{operation_id}"'
    query += """) {
        ... on BulkOperation {
          id
          status
          errorCode
          createdAt
          completedAt
          objectCount
          fileSize
          url
        }
      }
    }
    """
    
    response = requests.post(SHOPIFY_API_URL, headers=SHOPIFY_HEADERS, json={"query": query})
    return response.json()

def download_bulk_data(url):
    """Download the bulk operation results"""
    response = requests.get(url)
    return response.text

# Bulk query to fetch all products with their variants
bulk_query = """
{
  products {
    edges {
      node {
        id
        title
        handle
        description
        vendor
        productType
        tags
        status
        createdAt
        updatedAt
        variants {
          edges {
            node {
              id
              title
              sku
              price
              compareAtPrice
              inventoryQuantity
            }
          }
        }
        images {
          edges {
            node {
              id
              url
              altText
            }
          }
        }
      }
    }
  }
}
"""

print("Creating bulk operation...")
result = create_bulk_operation(bulk_query)

# Debug: Print the result to see what we're getting
print("Debug - API Response:")
print(json.dumps(result, indent=2))

if 'errors' in result:
    print("Error creating bulk operation:")
    print(json.dumps(result, indent=2))
    exit(1)

# Check if result['data'] exists and is not None
if not result.get('data'):
    print("Error: No 'data' in response")
    print("Full response:", result)
    exit(1)

# Check if the bulk operation data exists
if not result['data'].get('bulkOperationRunQuery'):
    print("Error: No 'bulkOperationRunQuery' in data")
    print("Data keys:", list(result['data'].keys()))
    exit(1)

bulk_operation = result['data']['bulkOperationRunQuery']['bulkOperation']
operation_id = bulk_operation['id']
status = bulk_operation['status']

print(f"Bulk operation created with ID: {operation_id}")
print(f"Initial status: {status}")

# Poll for completion
while status in ['CREATED', 'RUNNING']:
    print(f"Operation status: {status}")
    time.sleep(5)  # Wait 5 seconds before checking again
    
    result = check_bulk_operation_status(operation_id)
    
    # Debug: Print the status check result
    print("Debug - Status Check Response:")
    print(json.dumps(result, indent=2))
    
    if 'errors' in result:
        print("Error checking bulk operation status:")
        print(json.dumps(result, indent=2))
        exit(1)
    
    # Check if result['data'] exists and is not None
    if not result.get('data'):
        print("Error: No 'data' in status check response")
        print("Full response:", result)
        exit(1)
    
    node_data = result['data']['node']
    status = node_data['status']
    
    if status == 'COMPLETED':
        print("Bulk operation completed!")
        print(f"Objects processed: {node_data.get('objectCount', 'N/A')}")
        print(f"File size: {node_data.get('fileSize', 'N/A')} bytes")
        
        # Download the results
        if node_data.get('url'):
            print("Downloading results...")
            data = download_bulk_data(node_data['url'])
            
            # Save to file
            with open('bulk_products_data.jsonl', 'w') as f:
                f.write(data)
            print("Results saved to bulk_products_data.jsonl")
            
            # Parse and display sample data
            lines = data.strip().split('\n')
            if lines:
                sample = json.loads(lines[0])
                print("\nSample data:")
                print(json.dumps(sample, indent=2))
        break
    elif status == 'FAILED':
        print("Bulk operation failed!")
        print(f"Error code: {node_data.get('errorCode', 'N/A')}")
        exit(1)