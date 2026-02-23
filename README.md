# BigQuery Schema Migrator

Schema migration tool for BigQuery — runs SQL scripts in sequential order with idempotency control, similar to Flyway/Liquibase. Each script runs exactly once.

## How it works

- Migrations live in `migrations/` as `V00001_description.sql` files
- A `schema_migrations` control table in BigQuery tracks every execution (checksum SHA256, timing, success)
- Scripts with a `-- @scheduled` header are deployed as BigQuery Scheduled Queries via the Data Transfer API instead of being run directly
- If a migration fails, execution stops — subsequent scripts are not run

---

## Project structure

```
bigquery-schema-migrator/
├── migrate.py                  # Single entry point
├── credentials.py              # SA key auto-discovery
├── migrations/
│   ├── V00001_create_table_checklists.sql
│   ├── V00002_create_view_checklists.sql
│   ├── V00003_create_compaction_checklists.sql    # -- @scheduled
│   ├── V00004_create_table_jobperiods.sql
│   ├── V00005_create_view_jobperiods.sql
│   ├── V00006_create_compaction_jobperiods.sql    # -- @scheduled
│   ├── V00007_create_table_projectcyclecriteria.sql
│   ├── V00008_create_view_projectcyclecriteria.sql
│   └── V00009_create_compaction_projectcyclecriteria.sql  # -- @scheduled
├── requirements.txt
├── .env.example
└── credentials/                # gitignored — place your SA key here
```

---

## Local setup

```bash
# 1. Create and activate a virtual environment
python -m venv venv && source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your GCP_PROJECT_ID, GCP_DATASET_ID, GCP_LOCATION

# 4. Set up credentials (choose one):
#    Option A — drop your .json SA key into credentials/ (auto-discovered)
#    Option B — set GOOGLE_APPLICATION_CREDENTIALS in .env
```

### `.env` variables

| Variable | Description | Default |
|---|---|---|
| `GCP_PROJECT_ID` | GCP project ID | *(required)* |
| `GCP_DATASET_ID` | BigQuery dataset ID | *(required)* |
| `GCP_LOCATION` | Dataset region | `US` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to SA key (overrides auto-discovery) | — |

---

## Usage

```bash
# Check migration status (applied + pending)
python migrate.py --status

# Dry-run: see what would be applied without executing anything
python migrate.py --dry-run

# Apply all pending migrations
python migrate.py
```

All options can be passed as CLI args (they override `.env`):

```bash
python migrate.py --project my-project --dataset my_dataset --location us-east1
```

> Run `python migrate.py -h` or `python migrate.py --help` to see all available options.

---


## Migration file naming

```
V{5_digits}_{description}.sql
```

Examples:
- `V00001_create_table_checklists.sql`
- `V00002_create_view_checklists.sql`
- `V00010_add_column_foo.sql`

## SQL placeholders

Use `${PROJECT}` and `${DATASET}` in SQL — substituted at runtime:

```sql
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.my_table` ( ... );
```

## Scheduled queries

Add a `-- @scheduled` header to deploy via the Data Transfer API instead of executing directly:

```sql
-- @scheduled
-- @display_name: compaction_my_table
-- @schedule: every day 04:00
-- @description: Remove duplicate records

DELETE FROM `${PROJECT}.${DATASET}.my_table` WHERE ...;
```

---

## CI/CD

> **Note:** This repository does not include a CI/CD configuration. Bitbucket Pipelines setup (with deployment variables per environment) can be managed separately in the client-specific deployment repository.

---

## Authentication

| Context | Method |
|---|---|
| Local dev | Drop `.json` key in `credentials/` (auto-discovered) or set `GOOGLE_APPLICATION_CREDENTIALS` in `.env` |
| GKE / Cloud Run | Workload Identity (no key file needed) |