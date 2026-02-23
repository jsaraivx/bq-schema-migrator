"""
credentials.py — Auto-discovers the Service Account JSON key from the
credentials/ folder and sets GOOGLE_APPLICATION_CREDENTIALS before any
GCP client is initialized.

Priority order:
  1. GOOGLE_APPLICATION_CREDENTIALS already set in the environment / .env
  2. Single .json file found inside credentials/
  3. Error — cannot proceed without credentials

Returns:
  str — the absolute path to the resolved key file.
"""

import os
import glob


def load_credentials() -> str:
    """Resolve, set, and return the path to the Service Account JSON key."""

    # 1. Already explicitly set — respect it and do nothing.
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        print(f"[auth] Using credentials from environment: {path}")
        return path

    # 2. Auto-discover any .json file inside credentials/
    matches = glob.glob("credentials/*.json")

    if len(matches) == 1:
        path = os.path.abspath(matches[0])
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        print(f"[auth] Service Account key auto-discovered: {path}")
        return path

    if len(matches) > 1:
        raise RuntimeError(
            f"Multiple JSON files found in credentials/: {matches}\n"
            "Please keep only one Service Account key file, or set "
            "GOOGLE_APPLICATION_CREDENTIALS explicitly in your .env."
        )

    # 3. Nothing found anywhere — fail loudly.
    raise RuntimeError(
        "No Service Account key found.\n"
        "Options:\n"
        "  A) Place a single .json key file inside the credentials/ folder.\n"
        "  B) Set GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json in your .env file.\n"
        "Download your key at: GCP Console → IAM → Service Accounts → Keys."
    )
