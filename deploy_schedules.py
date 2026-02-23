import os
import glob
import argparse
from dotenv import load_dotenv
from credentials import load_credentials
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.struct_pb2 import Struct

# Load variables from .env file (if present). CLI args take precedence.
load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Deploy Scheduled Queries to BigQuery")
    parser.add_argument(
        "--project-id",
        default=os.getenv("GCP_PROJECT_ID"),
        help="GCP project ID (or set GCP_PROJECT_ID in .env)",
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("GCP_DATASET_ID"),
        help="BigQuery dataset ID (or set GCP_DATASET_ID in .env)",
    )
    parser.add_argument(
        "--location",
        default=os.getenv("GCP_LOCATION", "US"),
        help="BigQuery dataset location (or set GCP_LOCATION in .env, default: US)",
    )
    parser.add_argument(
        "--schedule",
        default=os.getenv("GCP_SCHEDULE", "every 1 hours"),
        help="Schedule expression (or set GCP_SCHEDULE in .env, default: every 1 hours)",
    )
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id is required (or set GCP_PROJECT_ID in .env)")
    if not args.dataset_id:
        parser.error("--dataset-id is required (or set GCP_DATASET_ID in .env)")

    # Auto-discover credentials from credentials/ folder (or use GOOGLE_APPLICATION_CREDENTIALS)
    load_credentials()

    # Client for the Data Transfer Service (responsible for Scheduled Queries)
    client = bigquery_datatransfer_v1.DataTransferServiceClient()

    # The parent represents the project + region.
    # Set GCP_LOCATION in .env or pass --location if your dataset is not in 'US'.
    parent = f"projects/{args.project_id}/locations/{args.location}"

    print("Starting Scheduled Queries deployment to BigQuery (Python)...")
    print(f"Target: {args.project_id}.{args.dataset_id} | Location: {args.location} | Schedule: {args.schedule}")

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
