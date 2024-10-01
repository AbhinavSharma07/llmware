# Copyright 2023-2024 llmware

# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""The setup module implements the init process.

The module implements the Setup class, which has three static methods:
- load_sample_files
- load_voice_sample_files
- load_selected_sample_files

These methods create the necessary directories if they do not exist and
download the sample files from an llmware-maintained AWS S3 instance.
"""

import shutil
import os
from pathlib import Path
from typing import Optional

import logging

from llmware.resources import CloudBucketManager
from llmware.configs import LLMWareConfig


logger = logging.getLogger(__name__)
config = LLMWareConfig()
logger.setLevel(level=config.get_logging_level_by_module(__name__))


class Setup:
    """Implements the download of sample files from an AWS S3 bucket.

    The `Setup` class provides methods to download various types of sample
    files from an AWS S3 bucket managed by llmware. These samples are
    categorized into different domains such as agreements, invoices, etc.

    Methods
    -------
    load_sample_files(over_write: bool = False) -> Path
        Downloads general sample document files.
    
    load_voice_sample_files(over_write: bool = False, small_only: bool = True) -> Path
        Downloads voice sample files.
    
    load_selected_sample_files(sample_folder: str = "microsoft_ir", over_write: bool = False) -> Path
        Downloads selected sample files based on the provided folder name.
    """

    @staticmethod
    def _download_and_unpack(
        folder_name: str,
        sample_files_path: Path,
        bucket_manager: CloudBucketManager,
        bucket_name: str
    ) -> None:
        """
        Downloads and unpacks a zip file from the S3 bucket.

        Parameters
        ----------
        folder_name : str
            The name of the folder (without .zip) to download.
        sample_files_path : Path
            The local path where the files will be stored.
        bucket_manager : CloudBucketManager
            An instance of CloudBucketManager to handle S3 operations.
        bucket_name : str
            The name of the S3 bucket.
        
        Raises
        ------
        RuntimeError
            If the download or extraction fails.
        """
        remote_zip = f"{folder_name}.zip"
        local_zip = sample_files_path / remote_zip

        try:
            logger.info(f"Downloading '{remote_zip}' from bucket '{bucket_name}'...")
            bucket_manager.pull_file_from_public_s3(remote_zip, str(local_zip), bucket_name)
            logger.info(f"Extracting '{local_zip}' to '{sample_files_path}'...")
            shutil.unpack_archive(str(local_zip), str(sample_files_path), "zip")
            logger.info(f"Removing temporary file '{local_zip}'...")
            local_zip.unlink()
            logger.info(f"Successfully downloaded and extracted '{folder_name}'.")
        except Exception as e:
            logger.error(f"Failed to download and extract '{folder_name}': {e}", exc_info=True)
            raise RuntimeError(f"Failed to download and extract '{folder_name}': {e}") from e

    @staticmethod
    def load_sample_files(over_write: bool = False) -> Path:
        """
        Downloads general sample document files from the AWS S3 bucket.

        Parameters
        ----------
        over_write : bool, optional
            If True, existing files will be overwritten. Default is False.

        Returns
        -------
        Path
            The local path where the sample files are stored.
        """
        workspace_path = Path(config.get_llmware_path())
        sample_files_path = workspace_path / "sample_files"
        bucket_name = config.get_config("llmware_sample_files_bucket")
        bucket_manager = CloudBucketManager()

        if not workspace_path.exists():
            logger.info(f"Workspace path '{workspace_path}' does not exist. Setting up workspace...")
            config.setup_llmware_workspace()

        if sample_files_path.exists() and not over_write:
            logger.info(f"Sample files path already exists and overwrite is disabled: '{sample_files_path}'.")
            return sample_files_path

        sample_files_path.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading general sample files from AWS S3 bucket...")
        Setup._download_and_unpack("sample_files", sample_files_path, bucket_manager, bucket_name)

        return sample_files_path

    @staticmethod
    def load_voice_sample_files(over_write: bool = False, small_only: bool = True) -> Path:
        """
        Downloads voice sample files from the AWS S3 bucket.

        Parameters
        ----------
        over_write : bool, optional
            If True, existing files will be overwritten. Default is False.
        small_only : bool, optional
            If True, only small voice sample files are downloaded. Otherwise, all voice samples are downloaded. Default is True.

        Returns
        -------
        Path
            The local path where the voice sample files are stored.
        """
        workspace_path = Path(config.get_llmware_path())
        folder_suffix = "voice_sample_files_small" if small_only else "voice_sample_files"
        sample_files_path = workspace_path / folder_suffix
        bucket_name = config.get_config("llmware_sample_files_bucket")
        bucket_manager = CloudBucketManager()

        if not workspace_path.exists():
            logger.info(f"Workspace path '{workspace_path}' does not exist. Setting up workspace...")
            config.setup_llmware_workspace()

        if sample_files_path.exists() and not over_write:
            logger.info(f"Voice sample files path already exists and overwrite is disabled: '{sample_files_path}'.")
            return sample_files_path

        sample_files_path.mkdir(parents=True, exist_ok=True)

        folder_name = "voice_small" if small_only else "voice_all"
        logger.info(f"Downloading {'small' if small_only else 'all'} voice sample files from AWS S3 bucket...")
        Setup._download_and_unpack(folder_name, sample_files_path, bucket_manager, bucket_name)

        return sample_files_path

    @staticmethod
    def load_selected_sample_files(sample_folder: str = "microsoft_ir", over_write: bool = False) -> Path:
        """
        Downloads selected sample files from the AWS S3 bucket based on the provided folder name.

        Parameters
        ----------
        sample_folder : str, optional
            The name of the sample folder to download. Default is "microsoft_ir".
        over_write : bool, optional
            If True, existing files will be overwritten. Default is False.

        Returns
        -------
        Path
            The local path where the selected sample files are stored.
        """
        workspace_path = Path(config.get_llmware_path())
        sample_files_path = workspace_path / sample_folder
        bucket_name = config.get_config("llmware_sample_files_bucket")
        bucket_manager = CloudBucketManager()

        if not workspace_path.exists():
            logger.info(f"Workspace path '{workspace_path}' does not exist. Setting up workspace...")
            config.setup_llmware_workspace()

        if sample_files_path.exists() and not over_write:
            logger.info(f"Selected sample files path already exists and overwrite is disabled: '{sample_files_path}'.")
            return sample_files_path

        sample_files_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading selected sample files '{sample_folder}' from AWS S3 bucket...")
        Setup._download_and_unpack(sample_folder, sample_files_path, bucket_manager, bucket_name)

        return sample_files_path
