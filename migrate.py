"""
migrate.py — BigQuery Schema Migrator

Usage:
    python migrate.py --project my-project --dataset my_dataset --location us-east1
    python migrate.py --project my-project --dataset my_dataset --location us-east1 --dry-run
    python migrate.py --project my-project --dataset my_dataset --location us-east1 --status
"""

import os
import glob
import hashlib
import time
import re

import click
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1
from google.protobuf.struct_pb2 import Struct

from credentials import load_credentials

# Load .env before anything else (CLI args take precedence via click defaults)
load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIGRATIONS_DIR = "migrations"
CONTROL_TABLE = "schema_migrations"

CREATE_CONTROL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.schema_migrations` (
  version        STRING    NOT NULL,
  script_name    STRING    NOT NULL,
  checksum       STRING    NOT NULL,
  executed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  execution_time_ms INT64,
  success        BOOL      NOT NULL
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def extract_version(filename: str) -> str:
    """Extract version token from filename, e.g. 'V00001' from 'V00001_create_foo.sql'."""
    basename = os.path.basename(filename)
    match = re.match(r"^(V\d+)_", basename)
    if not match:
        raise ValueError(f"Invalid migration filename format: {basename!r}. Expected: V00001_description.sql")
    return match.group(1)


def substitute_placeholders(sql: str, project: str, dataset: str) -> str:
    return sql.replace("${PROJECT}", project).replace("${DATASET}", dataset)


def is_scheduled(sql: str) -> bool:
    """Return True if the script starts with the -- @scheduled marker."""
    return sql.lstrip().startswith("-- @scheduled")


def parse_scheduled_metadata(sql: str) -> dict:
    """Parse -- @key: value header lines from a scheduled script."""
    meta = {}
    for line in sql.splitlines():
        line = line.strip()
        if not line.startswith("--"):
            break
        match = re.match(r"^--\s*@(\w+):\s*(.+)$", line)
        if match:
            meta[match.group(1)] = match.group(2).strip()
    return meta


# ---------------------------------------------------------------------------
# BigQuery operations
# ---------------------------------------------------------------------------

def ensure_control_table(client: bigquery.Client, project: str, dataset: str) -> None:
    sql = CREATE_CONTROL_TABLE_SQL.format(project=project, dataset=dataset)
    client.query(sql).result()


def get_executed_versions(client: bigquery.Client, project: str, dataset: str) -> set:
    """Return a set of versions that were successfully executed."""
    sql = f"""
        SELECT version
        FROM `{project}.{dataset}.{CONTROL_TABLE}`
        WHERE success = TRUE
    """
    try:
        rows = client.query(sql).result()
        return {row.version for row in rows}
    except Exception:
        # Table may not exist yet on first run — will be created by ensure_control_table
        return set()


def record_migration(
    client: bigquery.Client,
    project: str,
    dataset: str,
    version: str,
    script_name: str,
    checksum: str,
    execution_time_ms: int,
    success: bool,
) -> None:
    table_id = f"{project}.{dataset}.{CONTROL_TABLE}"
    rows = [
        {
            "version": version,
            "script_name": script_name,
            "checksum": checksum,
            "execution_time_ms": execution_time_ms,
            "success": success,
        }
    ]
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Failed to record migration {version}: {errors}")


def run_sql(client: bigquery.Client, sql: str, script_name: str) -> int:
    """Execute SQL and return elapsed milliseconds."""
    click.echo(f"  Running {script_name} ...")
    start = time.monotonic()
    client.query(sql).result()
    elapsed_ms = int((time.monotonic() - start) * 1000)
    return elapsed_ms


def deploy_scheduled_query(
    project: str,
    dataset: str,
    location: str,
    display_name: str,
    schedule: str,
    sql: str,
    script_name: str,
) -> None:
    """Create a BigQuery scheduled query via Data Transfer Service."""
    click.echo(f"  Creating scheduled query '{display_name}' ...")
    dt_client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = f"projects/{project}/locations/{location}"

    params = Struct()
    params.update({"query": sql})

    config = bigquery_datatransfer_v1.TransferConfig(
        destination_dataset_id=dataset,
        display_name=display_name,
        data_source_id="scheduled_query",
        params=params,
        schedule=schedule,
    )
    response = dt_client.create_transfer_config(
        request={"parent": parent, "transfer_config": config}
    )
    click.echo(f"  Scheduled query created: {response.name}")


# ---------------------------------------------------------------------------
# Core migration logic
# ---------------------------------------------------------------------------

def collect_migration_files() -> list[str]:
    files = sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "V*.sql")))
    return files


def run_migrations(
    client: bigquery.Client,
    project: str,
    dataset: str,
    location: str,
    dry_run: bool,
) -> None:
    files = collect_migration_files()
    if not files:
        click.echo(f"No migration files found in {MIGRATIONS_DIR}/")
        return

    if dry_run:
        # Offline: just show all scripts — we can't check the control table without connecting
        click.echo(f"Found {len(files)} migration file(s):")
        for f in files:
            click.echo(f"  → {os.path.basename(f)}")
        click.echo("\n[dry-run] No changes applied.")
        return

    ensure_control_table(client, project, dataset)
    executed = get_executed_versions(client, project, dataset)

    pending = [f for f in files if extract_version(f) not in executed]

    if not pending:
        click.echo("All migrations are already applied. Nothing to do.")
        return

    click.echo(f"Found {len(pending)} pending migration(s):")
    for f in pending:
        click.echo(f"  → {os.path.basename(f)}")

    click.echo("")
    for file_path in pending:
        version = extract_version(file_path)
        script_name = os.path.basename(file_path)

        with open(file_path, "r", encoding="utf-8") as fh:
            raw_sql = fh.read()

        checksum = sha256(raw_sql)
        sql = substitute_placeholders(raw_sql, project, dataset)

        try:
            if is_scheduled(raw_sql):
                meta = parse_scheduled_metadata(raw_sql)
                display_name = meta.get("display_name", script_name)
                schedule = meta.get("schedule", "every 24 hours")
                # Strip header comments, keep only the actual SQL
                body_lines = [
                    line for line in sql.splitlines()
                    if not line.strip().startswith("--")
                ]
                body_sql = "\n".join(body_lines).strip()
                start = time.monotonic()
                deploy_scheduled_query(project, dataset, location, display_name, schedule, body_sql, script_name)
                elapsed_ms = int((time.monotonic() - start) * 1000)
            else:
                elapsed_ms = run_sql(client, sql, script_name)

            record_migration(client, project, dataset, version, script_name, checksum, elapsed_ms, True)
            click.secho(f"  ✓ {script_name} ({elapsed_ms}ms)", fg="green")

        except Exception as exc:  # noqa: BLE001
            record_migration(client, project, dataset, version, script_name, checksum, 0, False)
            click.secho(f"\n✗ Migration failed: {script_name}", fg="red", err=True)
            click.secho(f"  {exc}", fg="red", err=True)
            raise SystemExit(1) from exc


def show_status(client: bigquery.Client, project: str, dataset: str) -> None:
    ensure_control_table(client, project, dataset)
    executed = get_executed_versions(client, project, dataset)

    sql = f"""
        SELECT version, script_name, executed_at, execution_time_ms, success
        FROM `{project}.{dataset}.{CONTROL_TABLE}`
        ORDER BY version
    """
    rows = list(client.query(sql).result())
    executed_map = {row.version: row for row in rows}

    files = collect_migration_files()

    click.echo(f"\n{'VERSION':<12} {'STATUS':<12} {'SCRIPT':<50} {'EXECUTED AT'}")
    click.echo("-" * 100)

    for file_path in files:
        version = extract_version(file_path)
        script_name = os.path.basename(file_path)
        if version in executed_map:
            row = executed_map[version]
            status = click.style("applied", fg="green") if row.success else click.style("failed", fg="red")
            executed_at = str(row.executed_at)[:19] if row.executed_at else "-"
        else:
            status = click.style("pending", fg="yellow")
            executed_at = "-"
        click.echo(f"{version:<12} {status:<21} {script_name:<50} {executed_at}")

    # Scripts in control table not in filesystem
    all_file_versions = {extract_version(f) for f in files}
    for version, row in executed_map.items():
        if version not in all_file_versions:
            status = click.style("orphan", fg="cyan")
            executed_at = str(row.executed_at)[:19] if row.executed_at else "-"
            click.echo(f"{version:<12} {status:<21} {row.script_name:<50} {executed_at}")

    click.echo("")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--project",  default=lambda: os.getenv("GCP_PROJECT_ID"),  required=True,  help="GCP project ID (or set GCP_PROJECT_ID in .env)")
@click.option("--dataset",  default=lambda: os.getenv("GCP_DATASET_ID"),  required=True,  help="BigQuery dataset ID (or set GCP_DATASET_ID in .env)")
@click.option("--location", default=lambda: os.getenv("GCP_LOCATION", "US"), show_default=True, help="Dataset location (or set GCP_LOCATION in .env)")
@click.option("--dry-run",  is_flag=True, default=False, help="Show pending migrations without executing")
@click.option("--status",   is_flag=True, default=False, help="Show applied and pending migrations")
def main(project: str, dataset: str, location: str, dry_run: bool, status: bool) -> None:
    """
    BigQuery Schema Migrator — applies SQL migrations in sequential order.

    Migrations in migrations/ are run once and tracked in the schema_migrations
    control table. Scripts with a -- @scheduled header are deployed as BigQuery
    Scheduled Queries instead of being executed directly.

    Configuration can be provided via CLI args or .env file:

    \b
      GCP_PROJECT_ID   GCP project ID
      GCP_DATASET_ID   BigQuery dataset ID
      GCP_LOCATION     Dataset region (default: US)

    Examples:

    \b
      python migrate.py --status
      python migrate.py --dry-run
      python migrate.py
      python migrate.py --project my-project --dataset my_dataset --location us-east1
    """

    load_credentials()
    client = bigquery.Client(project=project)

    click.echo(f"BigQuery Schema Migrator")
    click.echo(f"Target: {project}.{dataset} | Location: {location}")
    click.echo("-" * 60)

    if status:
        show_status(client, project, dataset)
        return

    run_migrations(client, project, dataset, location, dry_run)

    if not dry_run:
        click.secho("\nMigration completed successfully!", fg="green")


if __name__ == "__main__":
    main()
