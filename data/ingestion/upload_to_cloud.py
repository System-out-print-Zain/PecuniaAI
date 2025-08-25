import boto3
import os
from pathlib import Path
from typing import List, Tuple, Dict
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError
import json

"""
This script is used to take documents from the staging directory
and upload them to cloud storage
"""

load_dotenv()
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
s3_client = boto3.client("s3")

manifest_metadata = {}


def read_metadata(dir_path: str) -> Dict[str, str]:
    """
    Reads metadata for all files in <dir_path> and returns it as a dictionary.
    Precondition: <dir_path> must have a manifest.json
    """

    path = f"{dir_path}/manifest.json"
    try:
        with open(path, "r") as f:
            metadata = json.load(f)
            return metadata
    except FileNotFoundError:
        print("The file was not found.")
    except PermissionError:
        print("Permission denied.")
    except OSError as e:
        print(f"Unexpected OS error: {e}")


def upload_file(
    file_path: str, bucket_name: str, obj_key: str, metadata: Dict[str, str]
) -> bool:
    """
    Upload a file to S3.

    Args:
        file_path (str): The path to the file to upload.
        bucket_name (str): The name of the bucket to upload the file to.
        obj_key (str): The S3 object key for the file.

    Returns:
        bool: True if upload successful, False otherwise.
    """
    try:
        print(f"Uploading {file_path} to s3://{bucket_name}/{obj_key}...")
        s3_client.upload_file(
            file_path, bucket_name, obj_key, ExtraArgs={"Metadata": metadata}
        )
        print(f"Successfully uploaded {obj_key}")
        return True
    except FileNotFoundError:
        print(f"File {file_path} not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as e:
        print(f"Error uploading {file_path} to s3://{bucket_name}/{obj_key}: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return False


def get_all_files_recursive(dir_path: str) -> List[Path]:
    """
    Recursively get all files in a directory and its subdirectories.

    Args:
        dir_path (str): The directory path to search.

    Returns:
        List[Path]: List of Path objects for all files found.
    """
    dir_path_obj = Path(dir_path)
    if not dir_path_obj.exists():
        print(f"Directory {dir_path} does not exist")
        return []

    if not dir_path_obj.is_dir():
        print(f"{dir_path} is not a directory")
        return []

    files = []
    try:
        for item in dir_path_obj.rglob("*"):
            if item.is_file() and item.suffix.lower() == ".pdf":
                files.append(item)
    except PermissionError:
        print(f"Permission denied accessing {dir_path}")
    except Exception as e:
        print(f"Error scanning directory {dir_path}: {e}")

    return files


def upload_all_files(
    dir_path: str, bucket_name: str, prefix: str = ""
) -> Tuple[int, int]:
    """
    Recursively upload all files from a directory and its subdirectories to S3.

    Args:
        dir_path (str): The local directory path to upload files from.
        bucket_name (str): The S3 bucket name to upload files to.
        prefix (str): Optional prefix to add to S3 object keys.

    Returns:
        Tuple[int, int]: (uploaded_count, failed_count)
    """
    print(
        f"Starting recursive upload of documents from '{dir_path}' to S3 bucket '{bucket_name}'..."
    )

    # Get all files recursively
    all_files = get_all_files_recursive(dir_path)
    metadata = read_metadata(dir_path)

    if not all_files:
        print("No files found to upload")
        return 0, 0

    print(metadata)
    if not metadata:
        print("No manifest file found")

    print(f"Found {len(all_files)} files to upload")

    uploaded_count = 0
    failed_count = 0
    dir_path_obj = Path(dir_path)

    for file_path in all_files:
        try:
            relative_path = file_path.relative_to(dir_path_obj)

            s3_object_key = str(relative_path).replace(os.sep, "/")

            if prefix:
                s3_object_key = f"{prefix.rstrip('/')}/{s3_object_key}"

            if upload_file(
                str(file_path), bucket_name, s3_object_key, metadata[str(relative_path)]
            ):
                uploaded_count += 1
            else:
                failed_count += 1

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            failed_count += 1

    print(f"Upload completed: {uploaded_count} successful, {failed_count} failed")
    return uploaded_count, failed_count


if __name__ == "__main__":
    upload_all_files(
        dir_path="../document_staging",
        bucket_name="pecunia-ai-document-storage",
        prefix="sedar-documents",
    )
