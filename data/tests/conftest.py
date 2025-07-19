"""
Pytest configuration for the data ingestion tests
"""

import pytest
import tempfile
import os
import shutil
from pathlib import Path


@pytest.fixture(scope="function")
def temp_directory():
    """
    Create a temporary directory for testing.
    Automatically cleaned up after each test.
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def sample_files(temp_directory):
    """
    Create sample files in a temporary directory for testing.
    Returns a tuple of (directory_path, list_of_file_paths)
    """
    files = []

    # Create files in root directory
    root_files = ["file1.txt", "file2.pdf", "file3.doc"]
    for filename in root_files:
        file_path = os.path.join(temp_directory, filename)
        with open(file_path, "w") as f:
            f.write(f"test content for {filename}")
        files.append(file_path)

    # Create subdirectory with files
    subdir = os.path.join(temp_directory, "subdir")
    os.makedirs(subdir)
    subdir_files = ["subfile1.txt", "subfile2.pdf"]
    for filename in subdir_files:
        file_path = os.path.join(subdir, filename)
        with open(file_path, "w") as f:
            f.write(f"test content for {filename}")
        files.append(file_path)

    # Create nested subdirectory
    nested_dir = os.path.join(subdir, "nested")
    os.makedirs(nested_dir)
    nested_files = ["nested1.txt"]
    for filename in nested_files:
        file_path = os.path.join(nested_dir, filename)
        with open(file_path, "w") as f:
            f.write(f"test content for {filename}")
        files.append(file_path)

    return temp_directory, files


@pytest.fixture(scope="function")
def mock_s3_client():
    """
    Mock S3 client for testing.
    """
    import boto3
    from unittest.mock import Mock

    mock_client = Mock()
    mock_client.upload_file = Mock()
    return mock_client


@pytest.fixture(scope="function")
def test_bucket_name():
    """
    Test bucket name for S3 operations.
    """
    return "test-bucket-12345"


@pytest.fixture(scope="function")
def test_prefix():
    """
    Test prefix for S3 object keys.
    """
    return "test-prefix"


# Test markers
def pytest_configure(config):
    """
    Configure custom test markers.
    """
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "aws: mark test as requiring AWS credentials")


# Test collection hooks
def pytest_collection_modifyitems(config, items):
    """
    Modify test collection to add markers based on test names.
    """
    for item in items:
        # Mark tests with "integration" in the name as integration tests
        if "integration" in item.name.lower():
            item.add_marker(pytest.mark.integration)

        # Mark tests with "slow" in the name as slow tests
        if "slow" in item.name.lower():
            item.add_marker(pytest.mark.slow)

        # Mark tests that interact with AWS as requiring AWS credentials
        if any(keyword in item.name.lower() for keyword in ["s3", "aws", "upload"]):
            item.add_marker(pytest.mark.aws)
