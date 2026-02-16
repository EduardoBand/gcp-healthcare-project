"""
============================================================================================
File: add_dags_to_composer.py
Project: healthcare-project-487014
Layer: Orchestration / Deployment Utility

Description:
    Utility script responsible for uploading DAGs and supporting data files
    to a Google Cloud Composer environment (Cloud Storage bucket).

    The script:
        - Copies relevant files into a temporary directory
        - Ignores unnecessary files (e.g., tests, __init__.py)
        - Uploads only valid files to the Composer GCS bucket
        - Preserves directory structure under a target prefix (dags/ or data/)

    This script is intended to be used in CI/CD pipelines (e.g., Cloud Build)
    or manually during development deployments.
============================================================================================
"""

import argparse
import glob
import os
import tempfile
from shutil import copytree, ignore_patterns
from typing import Tuple, List

from google.cloud import storage


# ============================================================================================
# INTERNAL UTILITIES
# ============================================================================================

def _create_file_list(directory: str, name_replacement: str) -> Tuple[str, List[str]]:
    """
    Copy eligible files from a directory to a temporary location.

    Parameters
    ----------
    directory : str
        Source directory to copy from.
    name_replacement : str
        Prefix replacement used later when generating GCS paths.

    Returns
    -------
    Tuple[str, List[str]]
        - Temporary directory path
        - List of files to upload

    Notes
    -----
    - Ignores __init__.py and test files (*_test.py)
    - Returns empty values if directory does not exist
    """

    if not os.path.exists(directory):
        print(f"⚠️ Warning: Directory '{directory}' does not exist. Skipping upload.")
        return "", []

    temp_dir = tempfile.mkdtemp()

    files_to_ignore = ignore_patterns("__init__.py", "*_test.py")
    copytree(directory, f"{temp_dir}/", ignore=files_to_ignore, dirs_exist_ok=True)

    # Ensure only files (not directories) are returned
    files = [
        f for f in glob.glob(f"{temp_dir}/**", recursive=True)
        if os.path.isfile(f)
    ]

    return temp_dir, files


# ============================================================================================
# CORE UPLOAD LOGIC
# ============================================================================================

def upload_to_composer(directory: str, bucket_name: str, name_replacement: str) -> None:
    """
    Upload files from a local directory to a Composer GCS bucket.

    Parameters
    ----------
    directory : str
        Local directory containing files to upload.
    bucket_name : str
        Target GCS bucket name.
    name_replacement : str
        Target prefix in the bucket (e.g., 'dags/' or 'data/').
    """

    temp_dir, files = _create_file_list(directory, name_replacement)

    if not files:
        print(f"⚠️ No files found in '{directory}'. Skipping upload.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        file_gcs_path = file.replace(f"{temp_dir}/", name_replacement)

        try:
            blob = bucket.blob(file_gcs_path)
            blob.upload_from_filename(file)

            print(f"✅ Uploaded {file} to gs://{bucket_name}/{file_gcs_path}")

        except IsADirectoryError:
            print(f"⚠️ Skipping directory: {file}")

        except FileNotFoundError:
            print(f"❌ Error: {file} not found. Ensure directory structure is correct.")
            raise


# ============================================================================================
# ENTRYPOINT
# ============================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload DAGs and data to a Cloud Composer GCS bucket."
    )

    parser.add_argument("--dags_directory", help="Path to DAGs directory.")
    parser.add_argument("--dags_bucket", help="GCS bucket name for Composer environment.")
    parser.add_argument("--data_directory", help="Path to data directory.")

    args = parser.parse_args()

    print(args.dags_directory, args.dags_bucket, args.data_directory)

    # ----------------------------------------------------------------------------------------
    # Upload DAGs
    # ----------------------------------------------------------------------------------------
    if args.dags_directory and os.path.exists(args.dags_directory):
        upload_to_composer(args.dags_directory, args.dags_bucket, "dags/")
    else:
        print(f"⚠️ Skipping DAGs upload: '{args.dags_directory}' directory not found.")

    # ----------------------------------------------------------------------------------------
    # Upload Supporting Data
    # ----------------------------------------------------------------------------------------
    if args.data_directory and os.path.exists(args.data_directory):
        upload_to_composer(args.data_directory, args.dags_bucket, "data/")
    else:
        print(f"⚠️ Skipping Data upload: '{args.data_directory}' directory not found.")
