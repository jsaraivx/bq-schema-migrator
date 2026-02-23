"""
tests/test_migrate.py — unit tests for pure helper functions in migrate.py

These tests do NOT require a BigQuery connection or GCP credentials.
Run with: pytest
"""

import os
import json
import pytest

from migrate import (
    sha256,
    extract_version,
    substitute_placeholders,
    is_scheduled,
    parse_scheduled_metadata,
)
from credentials import load_credentials


# ---------------------------------------------------------------------------
# sha256
# ---------------------------------------------------------------------------

class TestSha256:
    def test_returns_string(self):
        result = sha256("hello")
        assert isinstance(result, str)

    def test_known_hash(self):
        # echo -n "hello" | sha256sum → 2cf24dba...
        assert sha256("hello") == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

    def test_same_input_same_output(self):
        assert sha256("abc") == sha256("abc")

    def test_different_inputs_different_hashes(self):
        assert sha256("foo") != sha256("bar")

    def test_empty_string(self):
        result = sha256("")
        assert len(result) == 64  # SHA-256 is always 64 hex chars


# ---------------------------------------------------------------------------
# extract_version
# ---------------------------------------------------------------------------

class TestExtractVersion:
    def test_basic(self):
        assert extract_version("V00001_create_table_orders.sql") == "V00001"

    def test_full_path(self):
        assert extract_version("migrations/V00042_add_column_foo.sql") == "V00042"

    def test_high_version_number(self):
        assert extract_version("V99999_some_migration.sql") == "V99999"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid migration filename"):
            extract_version("create_table.sql")

    def test_missing_v_prefix_raises(self):
        with pytest.raises(ValueError, match="Invalid migration filename"):
            extract_version("00001_create_table.sql")


# ---------------------------------------------------------------------------
# substitute_placeholders
# ---------------------------------------------------------------------------

class TestSubstitutePlaceholders:
    def test_replaces_project(self):
        sql = "SELECT * FROM `${PROJECT}.dataset.table`"
        result = substitute_placeholders(sql, project="my-project", dataset="my_dataset")
        assert "my-project" in result
        assert "${PROJECT}" not in result

    def test_replaces_dataset(self):
        sql = "SELECT * FROM `project.${DATASET}.table`"
        result = substitute_placeholders(sql, project="proj", dataset="ds")
        assert "ds" in result
        assert "${DATASET}" not in result

    def test_replaces_both(self):
        sql = "CREATE TABLE `${PROJECT}.${DATASET}.orders` (id STRING);"
        result = substitute_placeholders(sql, project="p", dataset="d")
        assert result == "CREATE TABLE `p.d.orders` (id STRING);"

    def test_no_placeholders(self):
        sql = "SELECT 1"
        assert substitute_placeholders(sql, project="p", dataset="d") == "SELECT 1"

    def test_multiple_occurrences(self):
        sql = "${PROJECT} and ${PROJECT}"
        result = substitute_placeholders(sql, project="x", dataset="y")
        assert result == "x and x"


# ---------------------------------------------------------------------------
# is_scheduled
# ---------------------------------------------------------------------------

class TestIsScheduled:
    def test_scheduled_script(self):
        sql = "-- @scheduled\n-- @display_name: foo\nDELETE FROM table;"
        assert is_scheduled(sql) is True

    def test_not_scheduled(self):
        sql = "CREATE TABLE foo (id STRING);"
        assert is_scheduled(sql) is False

    def test_leading_whitespace_ignored(self):
        sql = "\n\n-- @scheduled\nSELECT 1;"
        assert is_scheduled(sql) is True

    def test_scheduled_not_first_comment(self):
        sql = "-- some other comment\n-- @scheduled\nSELECT 1;"
        assert is_scheduled(sql) is False


# ---------------------------------------------------------------------------
# parse_scheduled_metadata
# ---------------------------------------------------------------------------

class TestParseScheduledMetadata:
    FULL_HEADER = (
        "-- @scheduled\n"
        "-- @display_name: compaction_orders\n"
        "-- @schedule: every day 03:00\n"
        "-- @description: Remove duplicates\n"
        "\n"
        "DELETE FROM `project.dataset.orders` WHERE 1=1;"
    )

    def test_parses_display_name(self):
        meta = parse_scheduled_metadata(self.FULL_HEADER)
        assert meta["display_name"] == "compaction_orders"

    def test_parses_schedule(self):
        meta = parse_scheduled_metadata(self.FULL_HEADER)
        assert meta["schedule"] == "every day 03:00"

    def test_parses_description(self):
        meta = parse_scheduled_metadata(self.FULL_HEADER)
        assert meta["description"] == "Remove duplicates"

    def test_ignores_sql_body(self):
        meta = parse_scheduled_metadata(self.FULL_HEADER)
        assert "DELETE" not in str(meta)

    def test_empty_header(self):
        meta = parse_scheduled_metadata("SELECT 1;")
        assert meta == {}


# ---------------------------------------------------------------------------
# credentials.load_credentials
# ---------------------------------------------------------------------------

class TestLoadCredentials:
    def test_respects_existing_env_var(self, tmp_path, monkeypatch):
        key_file = tmp_path / "sa_key.json"
        key_file.write_text("{}")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key_file))
        result = load_credentials()
        assert result == str(key_file)

    def test_auto_discovers_single_json(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        # Point credentials/ to our tmp dir
        key_file = tmp_path / "sa.json"
        key_file.write_text(json.dumps({"type": "service_account"}))
        monkeypatch.chdir(tmp_path)
        (tmp_path / "credentials").mkdir()
        (tmp_path / "credentials" / "sa.json").write_text("{}")
        result = load_credentials()
        assert result.endswith("sa.json")

    def test_raises_when_no_credentials(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        monkeypatch.chdir(tmp_path)
        (tmp_path / "credentials").mkdir()
        with pytest.raises(RuntimeError, match="No Service Account key found"):
            load_credentials()

    def test_raises_when_multiple_json_files(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        monkeypatch.chdir(tmp_path)
        creds_dir = tmp_path / "credentials"
        creds_dir.mkdir()
        (creds_dir / "key1.json").write_text("{}")
        (creds_dir / "key2.json").write_text("{}")
        with pytest.raises(RuntimeError, match="Multiple JSON files"):
            load_credentials()
