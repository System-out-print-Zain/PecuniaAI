import boto3
import os
from pathlib import Path
from typing import List, Tuple
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

"""
This script is used to take documents from the staging directory
and upload them to cloud storage
"""

load_dotenv()
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
s3_client = boto3.client("s3")


def upload_file(file_path: str, bucket_name: str, obj_key: str) -> bool:
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
        s3_client.upload_file(file_path, bucket_name, obj_key)
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
            if item.is_file():
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

    if not all_files:
        print("No files found to upload")
        return 0, 0

    print(f"Found {len(all_files)} files to upload")

    uploaded_count = 0
    failed_count = 0
    dir_path_obj = Path(dir_path)

    for file_path in all_files:
        try:
            # Calculate relative path from the base directory
            relative_path = file_path.relative_to(dir_path_obj)

            # Create S3 object key with proper path separators
            s3_object_key = str(relative_path).replace(os.sep, "/")

            # Add prefix if provided
            if prefix:
                s3_object_key = f"{prefix.rstrip('/')}/{s3_object_key}"

            # Upload the file
            if upload_file(str(file_path), bucket_name, s3_object_key):
                uploaded_count += 1
            else:
                failed_count += 1

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            failed_count += 1

    print(f"Upload completed: {uploaded_count} successful, {failed_count} failed")
    return uploaded_count, failed_count


def upload_with_progress(
    dir_path: str, bucket_name: str, prefix: str = "", batch_size: int = 10
) -> Tuple[int, int]:
    """
    Upload files with progress reporting and batch processing.

    Args:
        dir_path (str): The local directory path to upload files from.
        bucket_name (str): The S3 bucket name to upload files to.
        prefix (str): Optional prefix to add to S3 object keys.
        batch_size (int): Number of files to process before reporting progress.

    Returns:
        Tuple[int, int]: (uploaded_count, failed_count)
    """
    print(f"Starting recursive upload with progress reporting...")

    all_files = get_all_files_recursive(dir_path)

    if not all_files:
        print("No files found to upload")
        return 0, 0

    total_files = len(all_files)
    print(f"Found {total_files} files to upload")

    uploaded_count = 0
    failed_count = 0
    dir_path_obj = Path(dir_path)

    for i, file_path in enumerate(all_files, 1):
        try:
            # Calculate relative path from the base directory
            relative_path = file_path.relative_to(dir_path_obj)

            # Create S3 object key with proper path separators
            s3_object_key = str(relative_path).replace(os.sep, "/")

            # Add prefix if provided
            if prefix:
                s3_object_key = f"{prefix.rstrip('/')}/{s3_object_key}"

            # Upload the file
            if upload_file(str(file_path), bucket_name, s3_object_key):
                uploaded_count += 1
            else:
                failed_count += 1

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            failed_count += 1

        # Report progress every batch_size files
        if i % batch_size == 0 or i == total_files:
            progress = (i / total_files) * 100
            print(
                f"Progress: {i}/{total_files} ({progress:.1f}%) - "
                f"Uploaded: {uploaded_count}, Failed: {failed_count}"
            )

    print(f"Upload completed: {uploaded_count} successful, {failed_count} failed")
    return uploaded_count, failed_count


def main():
    """
    Main function to run the upload script.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Upload files to S3 recursively")
    parser.add_argument("dir_path", help="Local directory path to upload")
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET_NAME,
        help="S3 bucket name (defaults to S3_BUCKET_NAME env var)",
    )
    parser.add_argument("--prefix", default="", help="Prefix to add to S3 object keys")
    parser.add_argument(
        "--batch-size", type=int, default=10, help="Batch size for progress reporting"
    )
    parser.add_argument(
        "--progress", action="store_true", help="Show progress during upload"
    )

    args = parser.parse_args()

    if not args.bucket:
        print("Error: S3 bucket name not provided and S3_BUCKET_NAME env var not set")
        return 1

    try:
        if args.progress:
            uploaded, failed = upload_with_progress(
                args.dir_path, args.bucket, args.prefix, args.batch_size
            )
        else:
            uploaded, failed = upload_all_files(args.dir_path, args.bucket, args.prefix)

        if failed > 0:
            print(f"Warning: {failed} files failed to upload")
            return 1
        else:
            print(f"Successfully uploaded {uploaded} files")
            return 0

    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
