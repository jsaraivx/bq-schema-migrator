import os
import glob
import argparse
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.struct_pb2 import Struct

def main():
    parser = argparse.ArgumentParser(description="Deploy Scheduled Queries to BigQuery")
    parser.add_argument("--project-id", required=True, help="GCP project ID")
    parser.add_argument("--dataset-id", required=True, help="BigQuery dataset ID")
    parser.add_argument("--schedule", default="every 1 hours", help="Schedule expression")
    args = parser.parse_args()

    # Client for the Data Transfer Service (responsible for Scheduled Queries)
    client = bigquery_datatransfer_v1.DataTransferServiceClient()

    # The parent represents the project and region.
    # NOTE: If your dataset is in a different region (e.g. 'southamerica-east1'),
    # change 'US' below to match your dataset's location.
    parent = f"projects/{args.project_id}/locations/US"

    print("Starting Scheduled Queries deployment to BigQuery (Python)...")

    query_files = sorted(glob.glob("scheduled_queries/*.sql"))
    if not query_files:
        print("'scheduled_queries/' folder not found or empty.")
        return

    for file_path in query_files:
        filename = os.path.basename(file_path).replace(".sql", "")
        print(f"Configuring schedule for: {filename}")

        with open(file_path, 'r', encoding='utf-8') as f:
            query = f.read()
            query = query.replace("{project_id}", args.project_id)
            query = query.replace("{dataset_id}", args.dataset_id)

        # Set parameters as a dict via Protobuf Struct
        params = Struct()
        params.update({"query": query})

        transfer_config = bigquery_datatransfer_v1.TransferConfig(
            destination_dataset_id=args.dataset_id,
            display_name=f"Deduplication - {filename}",
            data_source_id="scheduled_query",
            params=params,
            schedule=args.schedule,
        )

        try:
            # Create the transfer (scheduled query) configuration
            response = client.create_transfer_config(
                request={
                    "parent": parent,
                    "transfer_config": transfer_config,
                }
            )
            print(f"Schedule for '{filename}' created successfully! (ID: {response.name})")
        except Exception as e:
            print(f"Error creating schedule for '{filename}':\n{e}")
            raise

    print("All scheduled queries have been deployed to GCP!")

if __name__ == "__main__":
    main()
