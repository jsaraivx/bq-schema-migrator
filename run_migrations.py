import os
import glob
import argparse
from dotenv import load_dotenv
from google.cloud import bigquery

# Load variables from .env file (if present). CLI args take precedence.
load_dotenv()

def run_query(client, query, file_name):
    print(f"Running {file_name}...")
    try:
        job = client.query(query)
        job.result()  # Wait for the job to complete
        print(f"Success: {file_name}")
    except Exception as e:
        print(f"\nError running {file_name}:\n{e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="BigQuery Schema Migrator")
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
    args = parser.parse_args()

    if not args.project_id:
        parser.error("--project-id is required (or set GCP_PROJECT_ID in .env)")
    if not args.dataset_id:
        parser.error("--dataset-id is required (or set GCP_DATASET_ID in .env)")

    # GOOGLE_APPLICATION_CREDENTIALS is read automatically from the environment
    # (set it in .env or export it in your shell before running)
    client = bigquery.Client(project=args.project_id)

    print("Starting BigQuery Schema Migrator (Python)...")
    print(f"Target: {args.project_id}.{args.dataset_id}")

    # 1. Run incremental migrations
    print("-" * 48)
    print("Running incremental migrations...")
    migration_files = sorted(glob.glob("migrations/*.sql"))
    if not migration_files:
        print("'migrations/' folder not found or empty. Skipping.")
    else:
        for file_path in migration_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                query = f.read()
                # Replace environment placeholders at runtime
                query = query.replace("{project_id}", args.project_id)
                query = query.replace("{dataset_id}", args.dataset_id)
                run_query(client, query, file_path)

    # 2. Run idempotent views
    print("-" * 48)
    print("Running idempotent views...")
    view_files = sorted(glob.glob("views/*.sql"))
    if not view_files:
        print("'views/' folder not found or empty. Skipping.")
    else:
        for file_path in view_files:
            with open(file_path, 'r', encoding='utf-8') as f:
                query = f.read()
                query = query.replace("{project_id}", args.project_id)
                query = query.replace("{dataset_id}", args.dataset_id)
                run_query(client, query, file_path)

    print("-" * 48)
    print("Migration completed successfully!")

if __name__ == "__main__":
    main()
