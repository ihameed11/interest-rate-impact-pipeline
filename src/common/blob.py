"""
Small helper for Azure Blob Storage, used by every Python step.

Blob storage works like folders of files in the cloud:
  container  = top-level folder (we use "raw" and "processed")
  blob       = one file inside it, e.g. "sec_statements/2020q1.zip"
"""
from pathlib import Path

from azure.storage.blob import BlobServiceClient

from src.common import config


def _service() -> BlobServiceClient:
    account = config.require("AZURE_STORAGE_ACCOUNT")
    key = config.require("AZURE_STORAGE_KEY")
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
        # Send big files in small 4 MB pieces, so one slow moment on a home
        # connection only costs one small piece, which is then retried.
        max_single_put_size=4 * 1024 * 1024,
        max_block_size=4 * 1024 * 1024,
        connection_timeout=60,
        read_timeout=300,
        retry_total=5,
    )


def ensure_container(container: str) -> None:
    """Create the container if it does not exist yet (safe to call every time)."""
    client = _service().get_container_client(container)
    if not client.exists():
        client.create_container()
        print(f"  Created container '{container}'")


def exists(container: str, blob_name: str) -> bool:
    """True if the file is already in blob storage."""
    return _service().get_blob_client(container, blob_name).exists()


def upload_file(local_path: Path, container: str, blob_name: str) -> None:
    """Upload one local file. Replaces the blob if it already exists."""
    client = _service().get_blob_client(container, blob_name)
    with open(local_path, "rb") as f:
        client.upload_blob(f, overwrite=True, max_concurrency=2)


def upload_bytes(data: bytes, container: str, blob_name: str) -> None:
    """Upload data that is already in memory (e.g. an API answer)."""
    _service().get_blob_client(container, blob_name).upload_blob(data, overwrite=True)


def list_blobs(container: str, prefix: str = ""):
    """Yield (name, size in bytes) for every blob under a prefix."""
    for b in _service().get_container_client(container).list_blobs(name_starts_with=prefix):
        yield b.name, b.size


def download_file(container: str, blob_name: str, local_path: Path) -> None:
    """Download one blob to a local file."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    client = _service().get_blob_client(container, blob_name)
    with open(local_path, "wb") as f:
        client.download_blob(max_concurrency=2).readinto(f)
