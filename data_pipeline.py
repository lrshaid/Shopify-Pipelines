import requests
import json
import time
import os
from config import SHOPIFY_API_URL, SHOPIFY_HEADERS
from queries import QUERIES

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

def run_bulk_operation(query_key: str, query_info: dict):
    """Run a single bulk operation and save results."""
    print(f"\n{'='*60}")
    print(f"Running: {query_info['name']}")
    print(f"Description: {query_info['description']}")
    print(f"{'='*60}")
    
    # Create output directory if it doesn't exist
    output_dir = "bulk_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    print("Creating bulk operation...")
    create_result = create_bulk_operation(query_info['query'])
    # Basic shape/transport error checks
    if not isinstance(create_result, dict):
        print("Error: Unexpected response type when creating bulk op:", type(create_result))
        return False

    if "errors" in create_result and create_result["errors"]:
        print("Error creating bulk operation (top-level errors):")
        print(json.dumps(create_result, indent=2))
        return False

    data_block = create_result.get("data")
    if not data_block:
        print("Error: Response did not include 'data' when creating bulk op.")
        print(json.dumps(create_result, indent=2))
        return False

    bulk_run = data_block.get("bulkOperationRunQuery")
    if not bulk_run:
        print("Error: 'data.bulkOperationRunQuery' missing or null.")
        print(json.dumps(create_result, indent=2))
        return False

    user_errors = bulk_run.get("userErrors") or []
    if user_errors:
        print("User errors returned when creating bulk operation:")
        print(json.dumps(user_errors, indent=2))
        return False

    bulk_operation = bulk_run.get("bulkOperation")
    if not bulk_operation:
        print("Error: 'data.bulkOperationRunQuery.bulkOperation' missing or null.")
        print(json.dumps(create_result, indent=2))
        return False

    operation_id = bulk_operation.get("id")
    status = bulk_operation.get("status")
    if not operation_id or not status:
        print("Error: Bulk operation 'id' or 'status' missing.")
        print(json.dumps(create_result, indent=2))
        return False

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
            return False

        if "errors" in status_result and status_result["errors"]:
            print("Error checking bulk operation status (top-level errors):")
            print(json.dumps(status_result, indent=2))
            return False

        status_data = status_result.get("data")
        if not status_data:
            print("Error: No 'data' in status check response.")
            print(json.dumps(status_result, indent=2))
            return False

        node_data = status_data.get("node")
        if not node_data:
            print("Error: 'data.node' missing or null in status check response.")
            print(json.dumps(status_result, indent=2))
            return False

        status = node_data.get("status")
        if not status:
            print("Error: 'status' missing in node data.")
            print(json.dumps(status_result, indent=2))
            return False

    if status == "COMPLETED":
        print("Bulk operation completed!")
        print(f"Objects processed: {node_data.get('objectCount', 'N/A')}")
        print(f"File size: {node_data.get('fileSize', 'N/A')} bytes")

        signed_url = node_data.get("url")
        if not signed_url:
            print("No results URL was returned despite COMPLETED status.")
            return False

        print("Downloading results...")
        data_text = download_bulk_data(signed_url)

        # Save to file
        filename = f"{output_dir}/{query_key}_data.jsonl"
        with open(filename, 'w') as f:
            f.write(data_text)
        print(f"Results saved to {filename}")
        # Show count of lines
        line_count = len([ln for ln in data_text.splitlines() if ln.strip()])
        print(f"Downloaded {line_count} JSONL lines.")
        return True

    elif status == "FAILED":
        print("Bulk operation failed!")
        print(f"Error code: {node_data.get('errorCode', 'N/A')}")
        return False
    else:
        # CANCELED or other terminal state
        print(f"Bulk operation ended with terminal status: {status}")
        return False

def main():
    """Run bulk operations for all queries."""
    print("Shopify Bulk Data Extraction")
    print("=" * 60)
    print(f"Total queries to run: {len(QUERIES)}")
    
    # Show available queries
    print("\nAvailable queries:")
    for i, (key, info) in enumerate(QUERIES.items(), 1):
        print(f"{i}. {key}: {info['name']}")
    
    # Ask user which queries to run
    print("\nOptions:")
    print("1. Run all queries")
    print("2. Run specific queries")
    print("3. Run single query")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1":
        # Run all queries
        queries_to_run = QUERIES
    elif choice == "2":
        # Run specific queries
        print("\nEnter query keys separated by commas (e.g., orders_with_line_items,customers):")
        keys_input = input().strip()
        selected_keys = [key.strip() for key in keys_input.split(",")]
        queries_to_run = {key: QUERIES[key] for key in selected_keys if key in QUERIES}
    elif choice == "3":
        # Run single query
        print("\nEnter query key:")
        key = input().strip()
        if key in QUERIES:
            queries_to_run = {key: QUERIES[key]}
        else:
            print(f"Query '{key}' not found!")
            return
    else:
        print("Invalid choice!")
        return
    
    print(f"\nRunning {len(queries_to_run)} queries...")
    
    # Track results
    successful = 0
    failed = 0
    
    for query_key, query_info in queries_to_run.items():
        try:
            if run_bulk_operation(query_key, query_info):
                successful += 1
            else:
                failed += 1
        except Exception as e:
            print(f"Exception occurred while running {query_key}: {e}")
            failed += 1
        
        # Wait between operations to avoid rate limiting
        if len(queries_to_run) > 1:
            print("Waiting 10 seconds before next operation...")
            time.sleep(10)
    
    print(f"\n{'='*60}")
    print("Bulk operations completed!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Results saved in 'bulk_data/' directory")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()