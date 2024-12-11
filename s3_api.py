
"""
https://yandex.cloud/ru/docs/storage/tools/boto
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
!pip install boto3
"""

import os
from typing import Any, Dict, List
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from contextlib import contextmanager

from app_logger import get_logger

logger = get_logger(__name__)


class S3Client:
    def __init__(self, config: dict) -> None:
        """
        Initialize the S3 client with configuration

        :param config: Configuration for the S3 client
        """
        self.config = config
        self.config["config"] = Config(s3={"addressing_style": "path"})

    @contextmanager
    def get_s3_client(self) -> Any:
        """Yield an S3 client configured with the instance"s configuration"""
        s3_client = boto3.client("s3", **self.config)
        try:
            yield s3_client
        finally:
            s3_client = None  # Ensure the client is cleaned up.

    def create_bucket(self, bucket_name: str) -> bool:
        """Create an S3 bucket

        Args:
            bucket_name (str): Name of bucket to create

        Returns:
            bool: True if bucket created, else False
        """
        try:
            with self.get_s3_client() as client:
                client.create_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            logger.error(f"Error creating bucket {bucket_name}: {e}")
            return False

    def list_buckets(self) -> List[str]:
        """Get list of buckets."""
        bucket_names: List[str] = []
        with self.get_s3_client() as s3_client:
            response = s3_client.list_buckets()
            if "Buckets" in response:
                bucket_names = [
                    bucket["Name"] for bucket in response["Buckets"]
                ]
        return bucket_names

    def delete_bucket(self, bucket_name: str) -> bool:
        """
        Delete an S3 bucket.

        Args:
            bucket_name (str): Name of bucket to delete

        Returns:
            bool: True if bucket deleted, else False
        """
        try:
            with self.get_s3_client() as client:
                client.delete_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            logger.error(f"Error deleting bucket {bucket_name}: %s", e)
            return False

    def list_objects(self, bucket_name: str) -> List[str]:
        """
        Get list of objects in the specified S3 bucket.

        Args:
            bucket_name (str): Name of bucket to list objects in

        Returns:
            List[str]: List of object names in the specified bucket
        """
        object_names: List[str] = []
        try:
            with self.get_s3_client() as client:
                response = client.list_objects(Bucket=bucket_name)
                object_names = [
                    obj["Key"] for obj in response.get("Contents", [])
                ]
        except ClientError as e:
            logger.error(f"Error listing objects in {bucket_name}: {e}")
        return object_names

    def get_object_metadata(
            self, bucket_name: str, object_name: str) -> Dict[str, Any]:
        """Get metadata for the specified object."""
        metadata: Dict[str, Any] = {}
        try:
            with self.get_s3_client() as client:
                metadata = client.head_object(
                    Bucket=bucket_name, Key=object_name)
        except ClientError as e:
            logger.error(f"Error getting metadata for {object_name}: {e}")
        return metadata

    def create_object(
            self, bucket_name: str, object_name: str, content: str
    ) -> bool:
        """Create an S3 object.

        Args:
            bucket_name (str): Bucket to create object in.
            object_name (str): Name of object.
            content (str): Object content.

        Returns:
            bool: True if object created, else False.
        """
        try:
            client = self.get_s3_client()
            client.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=content.encode("utf-8"))
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"Error creating object {object_name}: {e}")
            return False
        return True

    def upload_file(
            self, bucket_name: str, file_path: str, prefix: str = "") -> bool:
        """
        Upload a file to an S3 bucket.

        Args:
            bucket_name: Bucket to upload to.
            file_path: Local path to file.
            prefix: Optional prefix to add to the object name.

        Returns:
            bool: True if the file was uploaded, else False.
        """
        object_name = os.path.basename(file_path)
        if prefix:
            object_name = f"{prefix}{object_name}"

        try:
            with self.get_s3_client() as client, open(file_path, "rb") as file:
                client.upload_fileobj(file, bucket_name, object_name)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                f"Error uploading file {file_path} "
                f"to bucket {bucket_name}: {e}")
            return False
        return True

    def copy_object(
        self,
        source_bucket_name: str,
        dest_bucket_name: str,
        source_object_name: str,
        dest_object_name: str
    ) -> bool:
        """
        Copy an object from one S3 bucket to another.

        Args:
            source_bucket_name (str): The source bucket name.
            dest_bucket_name (str): The destination bucket name.
            source_object_name (str): The source object name.
            dest_object_name (str): The destination object name.

        Returns:
            bool: True if the object was copied, else False.
        """
        copy_source = {
            "Bucket": source_bucket_name,
            "Key": source_object_name
        }
        try:
            with self.get_s3_client() as client:
                client.copy_object(
                    Bucket=dest_bucket_name,
                    Key=dest_object_name,
                    CopySource=copy_source
                )
            return True
        except ClientError as e:
            logger.error(
                "Error copying object from bucket %s to bucket %s: %s",
                source_bucket_name,
                dest_bucket_name,
                e
            )
            return False

    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        """
        Delete an S3 object.

        Args:
            bucket_name (str): Bucket to delete object in.
            object_name (str): Name of object.

        Returns:
            bool: True if object deleted, else False.
        """
        try:
            with self.get_s3_client() as client:
                client.delete_object(Bucket=bucket_name, Key=object_name)
        except ClientError as e:
            logger.error(f"Error deleting object {object_name}: {e}")
            return False
        return True

    def download_object(
            self, bucket_name: str, object_key: str, target_path: str) -> bool:
        """Download an S3 object.

        Args:
            bucket_name (str): Bucket to download from.
            object_key (str): Key of object.
            target_path (str): Local path to download to.

        Returns:
            bool: True if object downloaded, else False.
        """
        try:
            with self.get_s3_client() as client:
                with open(target_path, "wb") as file:
                    client.download_fileobj(bucket_name, object_key, file)
        except ClientError as e:
            logger.error(f"Error downloading object {object_key}: {e}")
            return False
        return True

    def create_presigned_url(
            self, bucket_name: str, object_name: str,
            expiration_seconds: int = 3600) -> str | None:
        """Create a presigned URL to access an S3 object.

        Args:
            bucket_name (str): Name of the bucket containing the object.
            object_name (str): Name of the object.
            expiration_seconds (int): Defaults to 1 hour.

        Returns:
            str | None: The presigned URL, or None if an error occurs.
        """
        try:
            client = self.get_s3_client()
            params = {"Bucket": bucket_name, "Key": object_name}
            presigned_url = client.generate_presigned_url(
                "get_object", Params=params, ExpiresIn=expiration_seconds
            )
        except (ClientError, Exception) as e:
            logger.error(
                f"Error generating presigned URL for object {object_name}: {e}"
            )
            return None
        return presigned_url
