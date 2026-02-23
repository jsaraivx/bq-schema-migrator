# BigQuery Schema Migrator

> **Single Source of Truth** for schemas and routines in BigQuery — versioned, secure, and CI/CD-ready.

A minimalist framework combining **pure SQL** with **Python** to manage the lifecycle of tables, views, and Scheduled Queries in Google BigQuery. No ORMs, no IaC abstractions — just clean SQL orchestrated via the GCP official API.

> **Requirements:** Python 3.9+ · `google-cloud-bigquery` · `google-cloud-bigquery-datatransfer` · `python-dotenv`

---

## Table of Contents

- [Project Philosophy](#project-philosophy)
- [Folder Structure](#folder-structure)
- [Directory Business Rules](#directory-business-rules)
- [Setup and Installation](#setup-and-installation)
- [How to Run](#how-to-run)
- [Naming Conventions](#naming-conventions)
- [Adding New Objects](#adding-new-objects)
- [CI/CD](#cicd)
- [FAQ](#faq)

---

## Project Philosophy

| Principle | Decision |
|---|---|
| **Full control** | Pure SQL, no code generation |
| **Data safety** | `CREATE TABLE IF NOT EXISTS` and `ALTER TABLE` in migrations — never `CREATE OR REPLACE TABLE` |
| **Idempotency** | `CREATE OR REPLACE VIEW` ensures safe re-execution with no side effects |
| **Traceability** | Versioned prefixes (`V1__`, `V2__`) guarantee clear execution order and history |
| **Environment flexibility** | `{project_id}` and `{dataset_id}` placeholders in SQL, resolved at runtime by Python |

---

## Folder Structure

```
bigquery-schema-migrator/
├── credentials/                # Service Account JSON key (git-ignored)
│   └── your-key.json           # Any filename — auto-discovered at runtime
├── migrations/                 # Incremental DDLs (tables, ALTER TABLE)
│   └── V1__create_*.sql
├── views/                      # Idempotent views (dedup, transformations)
│   └── v_*.sql
├── scheduled_queries/          # Scheduled DMLs (cleanup, deduplication)
│   └── *.sql
├── credentials.py              # Auth helper: auto-discovers SA key from credentials/
├── run_migrations.py           # Orchestrator: runs migrations + views
├── deploy_schedules.py         # Orchestrator: creates/updates Scheduled Queries in GCP
├── .env.example                # Environment variable template
└── requirements.txt
```

---

## Directory Business Rules

### `migrations/` — Incremental Scripts

- **Used for:** table creation and schema changes (`ALTER TABLE`)
- **Naming pattern:** `V{n}__{description}.sql` (e.g. `V1__create_users.sql`, `V2__add_column_status.sql`)
- **Execution order:** alphabetical — the `V1__`, `V2__` prefix guarantees correct sequence
- **⛔ FORBIDDEN:** `CREATE OR REPLACE TABLE` — would destroy historical data from append-only tables
- **✅ REQUIRED:** `CREATE TABLE IF NOT EXISTS` for new tables; `ALTER TABLE` for changes

### `views/` — Idempotent Scripts

- **Used for:** deduplication views, transformations, reports
- **Naming pattern:** `v_{base_table_name}.sql`
- **✅ REQUIRED:** `CREATE OR REPLACE VIEW` — ensures full idempotency (safe to re-run at any time)

### `scheduled_queries/` — Scheduled DMLs

- **Used for:** cleanup and deduplication routines on the physical table (e.g. `DELETE` dedup)
- **Naming pattern:** `{action}_{table}.sql` (e.g. `cleanup_projectcyclecriteria.sql`)
- **Deployment:** managed by `deploy_schedules.py` via BigQuery Data Transfer API

---

## Setup and Installation

### Prerequisites

- Python 3.9+
- A **Service Account** in GCP with the following permissions on the target project/dataset:
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`
  - `roles/bigquery.dataViewer`
  - `roles/bigquerydatatransfer.editor` *(only required for `deploy_schedules.py`)*

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/your-username/bigquery-schema-migrator.git
cd bigquery-schema-migrator

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# or: .venv\Scripts\activate  # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up your environment variables
cd bigquery-schema-migrator
cp .env.example .env
# Edit .env with your values
```

### Service Account Authentication

The scripts use `credentials.py` to resolve the Service Account key automatically. The priority order is:

| Priority | Source | Description |
|---|---|---|
| 1st | `GOOGLE_APPLICATION_CREDENTIALS` env var | Set explicitly in `.env` or shell — always wins |
| 2nd | `credentials/*.json` (auto-discovery) | Drop **any** `.json` file in `credentials/` — filename doesn't matter |
| Error | Nothing found | Script fails with a descriptive message |

> **Security:** `credentials/` is listed in `.gitignore`. JSON key files inside it will never be committed to the repository.

**For local development (Option A — recommended):**
```bash
# Just drop your key file into the folder:
cp ~/Downloads/my-sa-key.json credentials/
# No .env change needed — it will be discovered automatically.
```

**For CI/CD (Option B):**
```bash
# Set GOOGLE_APPLICATION_CREDENTIALS via your pipeline's secret manager,
# or use a provider like google-github-actions/auth (see CI/CD section).
```

### Environment Variables (`.env`)

All configuration can be set in a `.env` file at the project root. CLI arguments always take precedence over `.env` values.

| Variable | Required | Default | Description |
|---|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | ☑️ | auto | Path to SA JSON key — optional if `credentials/*.json` exists |
| `GCP_PROJECT_ID` | ✅ | — | GCP project ID |
| `GCP_DATASET_ID` | ✅ | — | BigQuery dataset ID |
| `GCP_LOCATION` | ☑️ | `US` | Dataset region (e.g. `southamerica-east1`) |
| `GCP_SCHEDULE` | ☑️ | `every 1 hours` | Scheduled Query frequency |

```bash
cp .env.example .env
# Fill in GCP_PROJECT_ID and GCP_DATASET_ID at minimum
```

---

## How to Run

### Run Migrations and Views

Runs all files in `migrations/` (in order), then all files in `views/`.

```bash
cd bigquery-schema-migrator

# Option A: using .env (recommended for local dev)
python run_migrations.py

# Option B: passing args explicitly (recommended for CI/CD)
python run_migrations.py \
  --project-id=YOUR_GCP_PROJECT \
  --dataset-id=YOUR_DATASET
```

### Deploy Scheduled Queries

Creates the scheduled routines in the BigQuery Data Transfer Service.

```bash
cd bigquery-schema-migrator

# Option A: using .env
python deploy_schedules.py

# Option B: passing args explicitly
python deploy_schedules.py \
  --project-id=YOUR_GCP_PROJECT \
  --dataset-id=YOUR_DATASET \
  --location=southamerica-east1 \
  --schedule="every 1 hours"
```

---

## Naming Conventions

| Object | Convention | Example |
|---|---|---|
| Table migration | `V{n}__{action}_{table}.sql` | `V1__create_orders.sql` |
| Schema change | `V{n}__add_{column}_{table}.sql` | `V2__add_status_orders.sql` |
| View | `v_{base_table}.sql` | `v_orders.sql` |
| Scheduled Query | `{action}_{table}.sql` | `cleanup_orders.sql` |

---

## Adding New Objects

### New Table

1. Create `migrations/V{next_number}__create_{table}.sql`
2. Use `CREATE TABLE IF NOT EXISTS \`{project_id}.{dataset_id}.{table}\``
3. (Optional) Create `views/v_{table}.sql` for deduplication using `CREATE OR REPLACE VIEW`
4. (Optional) Create `scheduled_queries/cleanup_{table}.sql` with the DML cleanup logic

### Schema Change

1. Create `migrations/V{next_number}__add_{column}_{table}.sql`
2. Use `ALTER TABLE \`{project_id}.{dataset_id}.{table}\` ADD COLUMN IF NOT EXISTS ...`

### New View

1. Create `views/v_{table}.sql`
2. Use `CREATE OR REPLACE VIEW \`{project_id}.{dataset_id}.v_{table}\``

---

## CI/CD

Example workflow with **GitHub Actions**:

```yaml
# .github/workflows/migrate.yml
name: BigQuery Schema Migration

on:
  push:
    branches: [main]

jobs:
  migrate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r bigquery-schema-migrator/requirements.txt

      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      - name: Run Migrations
        run: |
          cd bigquery-schema-migrator
          python run_migrations.py \
            --project-id=${{ vars.GCP_PROJECT_ID }} \
            --dataset-id=${{ vars.GCP_DATASET_ID }}

      - name: Deploy Scheduled Queries
        run: |
          cd bigquery-schema-migrator
          python deploy_schedules.py \
            --project-id=${{ vars.GCP_PROJECT_ID }} \
            --dataset-id=${{ vars.GCP_DATASET_ID }}
```

**Secrets/Variables in GitHub:**
- `GCP_SA_KEY` → Full Service Account JSON *(Secret)*
- `GCP_PROJECT_ID` → GCP project ID *(Variable)*
- `GCP_DATASET_ID` → BigQuery dataset ID *(Variable)*

---

## FAQ

**Why pure SQL instead of Terraform/dbt?**
> Full control with no abstractions. The SQL you write is exactly what runs in BigQuery — no surprises. Ideal for teams that know BigQuery's syntax well and want to keep the stack simple.

**Why Python instead of pure Bash?**
> Python provides structured error handling, elegant environment placeholder substitution, native integration with `google-cloud-bigquery` and `google-cloud-bigquery-datatransfer` libraries, and works out of the box in any CI/CD runner without extra dependencies.

**Can I ever use `CREATE OR REPLACE TABLE`?**
> Never inside `migrations/`. That directory is exclusively for append-only tables (e.g. Kafka ingestion). `CREATE OR REPLACE TABLE` would delete all historical data. For full table recreations, prefer `DROP TABLE IF EXISTS` + `CREATE TABLE IF NOT EXISTS` in manually controlled scripts, outside of the pipeline.

**What are the `{project_id}` and `{dataset_id}` placeholders in the SQL files?**
> They are variables replaced at runtime by Python before sending the query to BigQuery. This allows the same SQL files to be used across multiple environments (dev, staging, prod) without modifying the SQL source files.
