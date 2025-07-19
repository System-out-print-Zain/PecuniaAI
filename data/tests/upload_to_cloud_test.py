"""
Pytest-based tests for upload_to_cloud module
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os
import shutil
from pathlib import Path
import sys

# Add the parent directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

from upload_to_cloud import (
    upload_file,
    get_all_files_recursive,
    upload_all_files,
    upload_with_progress,
)


class TestUploadFilePytest:
    """Pytest-based tests for upload_file function"""

    @pytest.mark.unit
    @pytest.mark.aws
    @patch("upload_to_cloud.s3_client")
    def test_upload_file_success(self, mock_s3_client, test_bucket_name):
        """Test successful file upload"""
        # Arrange
        test_file_path = "/tmp/test_file.txt"
        test_obj_key = "test/path/file.txt"
        mock_s3_client.upload_file.return_value = None

        # Act
        result = upload_file(test_file_path, test_bucket_name, test_obj_key)

        # Assert
        assert result is True
        mock_s3_client.upload_file.assert_called_once_with(
            test_file_path, test_bucket_name, test_obj_key
        )

    @pytest.mark.unit
    @pytest.mark.aws
    @patch("upload_to_cloud.s3_client")
    def test_upload_file_file_not_found(self, mock_s3_client, test_bucket_name):
        """Test upload when file doesn't exist"""
        # Arrange
        test_file_path = "/tmp/nonexistent_file.txt"
        test_obj_key = "test/path/file.txt"
        mock_s3_client.upload_file.side_effect = FileNotFoundError("File not found")

        # Act
        result = upload_file(test_file_path, test_bucket_name, test_obj_key)

        # Assert
        assert result is False
        mock_s3_client.upload_file.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.aws
    @patch("upload_to_cloud.s3_client")
    def test_upload_file_no_credentials(self, mock_s3_client, test_bucket_name):
        """Test upload when AWS credentials are not available"""
        # Arrange
        from botocore.exceptions import NoCredentialsError

        test_file_path = "/tmp/test_file.txt"
        test_obj_key = "test/path/file.txt"
        mock_s3_client.upload_file.side_effect = NoCredentialsError()

        # Act
        result = upload_file(test_file_path, test_bucket_name, test_obj_key)

        # Assert
        assert result is False
        mock_s3_client.upload_file.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.aws
    @patch("upload_to_cloud.s3_client")
    def test_upload_file_client_error(self, mock_s3_client, test_bucket_name):
        """Test upload when S3 client error occurs"""
        # Arrange
        from botocore.exceptions import ClientError

        test_file_path = "/tmp/test_file.txt"
        test_obj_key = "test/path/file.txt"
        mock_s3_client.upload_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
            "PutObject",
        )

        # Act
        result = upload_file(test_file_path, test_bucket_name, test_obj_key)

        # Assert
        assert result is False
        mock_s3_client.upload_file.assert_called_once()


class TestGetAllFilesRecursivePytest:
    """Pytest-based tests for get_all_files_recursive function"""

    @pytest.mark.unit
    def test_get_all_files_recursive_empty_directory(self, temp_directory):
        """Test getting files from empty directory"""
        # Act
        result = get_all_files_recursive(temp_directory)

        # Assert
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_all_files_recursive_with_files(self, sample_files):
        """Test getting files from directory with files"""
        # Arrange
        dir_path, file_paths = sample_files

        # Act
        result = get_all_files_recursive(dir_path)

        # Assert
        assert len(result) == 6  # 3 root + 2 subdir + 1 nested
        result_filenames = [f.name for f in result]
        expected_filenames = [
            "file1.txt",
            "file2.pdf",
            "file3.doc",
            "subfile1.txt",
            "subfile2.pdf",
            "nested1.txt",
        ]
        for filename in expected_filenames:
            assert filename in result_filenames

    @pytest.mark.unit
    def test_get_all_files_recursive_nonexistent_directory(self):
        """Test getting files from non-existent directory"""
        # Arrange
        nonexistent_dir = "/nonexistent/directory/path"

        # Act
        result = get_all_files_recursive(nonexistent_dir)

        # Assert
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_all_files_recursive_file_path(self, temp_directory):
        """Test getting files when path is a file, not directory"""
        # Arrange
        test_file = os.path.join(temp_directory, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test content")

        # Act
        result = get_all_files_recursive(test_file)

        # Assert
        assert len(result) == 0

    @pytest.mark.unit
    def test_get_all_files_recursive_permission_error(self, temp_directory):
        """Test getting files when permission is denied"""
        # Arrange
        restricted_dir = os.path.join(temp_directory, "restricted")
        os.makedirs(restricted_dir)

        # Mock the rglob method to raise PermissionError
        with patch("pathlib.Path.rglob") as mock_rglob:
            mock_rglob.side_effect = PermissionError("Permission denied")

            # Act
            result = get_all_files_recursive(temp_directory)

            # Assert
            assert len(result) == 0


class TestUploadAllFilesPytest:
    """Pytest-based tests for upload_all_files function"""

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_success(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test successful upload of all files"""
        # Arrange
        test_files = [
            Path(temp_directory) / "file1.txt",
            Path(temp_directory) / "subdir" / "file2.pdf",
        ]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True

        # Act
        uploaded, failed = upload_all_files(temp_directory, test_bucket_name)

        # Assert
        assert uploaded == 2
        assert failed == 0
        assert mock_upload.call_count == 2

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_with_prefix(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name, test_prefix
    ):
        """Test upload with prefix"""
        # Arrange
        test_files = [Path(temp_directory) / "file1.txt"]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True

        # Act
        uploaded, failed = upload_all_files(
            temp_directory, test_bucket_name, test_prefix
        )

        # Assert
        assert uploaded == 1
        assert failed == 0
        # Check that the prefix was added to the S3 object key
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args[0]
        assert test_prefix in call_args[2]  # obj_key should contain prefix

    @pytest.mark.unit
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_no_files(
        self, mock_get_files, temp_directory, test_bucket_name
    ):
        """Test upload when no files are found"""
        # Arrange
        mock_get_files.return_value = []

        # Act
        uploaded, failed = upload_all_files(temp_directory, test_bucket_name)

        # Assert
        assert uploaded == 0
        assert failed == 0

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_some_failures(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test upload when some files fail to upload"""
        # Arrange
        test_files = [
            Path(temp_directory) / "file1.txt",
            Path(temp_directory) / "file2.txt",
            Path(temp_directory) / "file3.txt",
        ]
        mock_get_files.return_value = test_files
        # First upload succeeds, second fails, third succeeds
        mock_upload.side_effect = [True, False, True]

        # Act
        uploaded, failed = upload_all_files(temp_directory, test_bucket_name)

        # Assert
        assert uploaded == 2
        assert failed == 1
        assert mock_upload.call_count == 3


class TestUploadWithProgressPytest:
    """Pytest-based tests for upload_with_progress function"""

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_with_progress_success(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test successful upload with progress reporting"""
        # Arrange
        test_files = [
            Path(temp_directory) / "file1.txt",
            Path(temp_directory) / "file2.txt",
            Path(temp_directory) / "file3.txt",
        ]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True
        batch_size = 2

        # Act
        uploaded, failed = upload_with_progress(
            temp_directory, test_bucket_name, batch_size=batch_size
        )

        # Assert
        assert uploaded == 3
        assert failed == 0
        assert mock_upload.call_count == 3

    @pytest.mark.unit
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_with_progress_no_files(
        self, mock_get_files, temp_directory, test_bucket_name
    ):
        """Test upload with progress when no files are found"""
        # Arrange
        mock_get_files.return_value = []

        # Act
        uploaded, failed = upload_with_progress(temp_directory, test_bucket_name)

        # Assert
        assert uploaded == 0
        assert failed == 0

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_with_progress_with_prefix(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name, test_prefix
    ):
        """Test upload with progress and prefix"""
        # Arrange
        test_files = [Path(temp_directory) / "file1.txt"]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True

        # Act
        uploaded, failed = upload_with_progress(
            temp_directory, test_bucket_name, prefix=test_prefix
        )

        # Assert
        assert uploaded == 1
        assert failed == 0
        # Check that the prefix was added to the S3 object key
        call_args = mock_upload.call_args[0]
        assert test_prefix in call_args[2]  # obj_key should contain prefix


class TestEdgeCasesPytest:
    """Pytest-based tests for edge cases and error conditions"""

    @pytest.mark.unit
    def test_get_all_files_recursive_symlinks(self, temp_directory):
        """Test handling of symbolic links"""
        # Arrange
        # Create a file
        original_file = os.path.join(temp_directory, "original.txt")
        with open(original_file, "w") as f:
            f.write("original content")

        # Create a symlink
        symlink_file = os.path.join(temp_directory, "symlink.txt")
        os.symlink(original_file, symlink_file)

        # Act
        result = get_all_files_recursive(temp_directory)

        # Assert
        assert len(result) == 2  # Both original and symlink should be found
        result_names = [f.name for f in result]
        assert "original.txt" in result_names
        assert "symlink.txt" in result_names

    @pytest.mark.unit
    def test_get_all_files_recursive_hidden_files(self, temp_directory):
        """Test handling of hidden files"""
        # Arrange
        hidden_file = os.path.join(temp_directory, ".hidden.txt")
        with open(hidden_file, "w") as f:
            f.write("hidden content")

        visible_file = os.path.join(temp_directory, "visible.txt")
        with open(visible_file, "w") as f:
            f.write("visible content")

        # Act
        result = get_all_files_recursive(temp_directory)

        # Assert
        assert len(result) == 2  # Both hidden and visible files should be found
        result_names = [f.name for f in result]
        assert ".hidden.txt" in result_names
        assert "visible.txt" in result_names

    @pytest.mark.unit
    def test_get_all_files_recursive_empty_subdirectories(self, temp_directory):
        """Test handling of empty subdirectories"""
        # Arrange
        empty_subdir = os.path.join(temp_directory, "empty_subdir")
        os.makedirs(empty_subdir)

        # Create a file in root
        root_file = os.path.join(temp_directory, "root.txt")
        with open(root_file, "w") as f:
            f.write("root content")

        # Act
        result = get_all_files_recursive(temp_directory)

        # Assert
        assert len(result) == 1  # Only the root file should be found
        assert result[0].name == "root.txt"

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_empty_prefix(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test upload with empty prefix"""
        # Arrange
        test_files = [Path(temp_directory) / "file1.txt"]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True

        # Act
        uploaded, failed = upload_all_files(temp_directory, test_bucket_name, "")

        # Assert
        assert uploaded == 1
        assert failed == 0
        # Check that no prefix was added
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args[0]
        obj_key = call_args[2]
        assert not obj_key.startswith("/")
        assert obj_key == "file1.txt"

    @pytest.mark.integration
    @pytest.mark.aws
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_all_files_prefix_with_slash(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test upload with prefix that has trailing slash"""
        # Arrange
        test_files = [Path(temp_directory) / "file1.txt"]
        mock_get_files.return_value = test_files
        mock_upload.return_value = True
        prefix_with_slash = "test-prefix/"

        # Act
        uploaded, failed = upload_all_files(
            temp_directory, test_bucket_name, prefix_with_slash
        )

        # Assert
        assert uploaded == 1
        assert failed == 0
        # Check that prefix was properly handled
        mock_upload.assert_called_once()
        call_args = mock_upload.call_args[0]
        obj_key = call_args[2]
        assert obj_key == "test-prefix/file1.txt"  # No double slash


class TestPerformancePytest:
    """Pytest-based tests for performance and large file sets"""

    @pytest.mark.slow
    @pytest.mark.integration
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_large_number_of_files(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test upload with a large number of files"""
        # Arrange
        # Create many test files
        test_files = []
        for i in range(100):
            test_files.append(Path(temp_directory) / f"file_{i:03d}.txt")

        mock_get_files.return_value = test_files
        mock_upload.return_value = True

        # Act
        uploaded, failed = upload_all_files(temp_directory, test_bucket_name)

        # Assert
        assert uploaded == 100
        assert failed == 0
        assert mock_upload.call_count == 100

    @pytest.mark.slow
    @pytest.mark.integration
    @patch("upload_to_cloud.upload_file")
    @patch("upload_to_cloud.get_all_files_recursive")
    def test_upload_with_progress_large_batch(
        self, mock_get_files, mock_upload, temp_directory, test_bucket_name
    ):
        """Test upload with progress reporting for large batch"""
        # Arrange
        test_files = []
        for i in range(50):
            test_files.append(Path(temp_directory) / f"file_{i:03d}.txt")

        mock_get_files.return_value = test_files
        mock_upload.return_value = True
        batch_size = 10

        # Act
        uploaded, failed = upload_with_progress(
            temp_directory, test_bucket_name, batch_size=batch_size
        )

        # Assert
        assert uploaded == 50
        assert failed == 0
        assert mock_upload.call_count == 50
