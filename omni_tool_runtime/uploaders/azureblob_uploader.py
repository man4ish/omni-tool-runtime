# omni_tool_runtime/uploaders/azureblob_uploader.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class AzureBlobUploader:
    account_name: str
    auth: str = "managed_identity"  # managed_identity | connection_string
    connection_string: Optional[str] = None

    def _client(self):
        try:
            from azure.storage.blob import BlobServiceClient
        except Exception as e:
            raise RuntimeError(f"azure-storage-blob not installed (install omnibioai-tool-runtime[azure]): {e}")

        if self.auth == "connection_string":
            if not self.connection_string:
                raise RuntimeError("AzureBlobUploader: connection_string auth requires connection_string")
            return BlobServiceClient.from_connection_string(self.connection_string)

        # managed identity / az login / workload identity
        try:
            from azure.identity import DefaultAzureCredential
        except Exception as e:
            raise RuntimeError(f"azure-identity not installed (install omnibioai-tool-runtime[azure]): {e}")

        cred = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        url = f"https://{self.account_name}.blob.core.windows.net"
        return BlobServiceClient(account_url=url, credential=cred)

    def upload_bytes(self, *, container: str, blob_path: str, data: bytes, content_type: str) -> None:
        svc = self._client()
        bc = svc.get_blob_client(container=container, blob=blob_path)
        # overwrite is fine for deterministic runs
        bc.upload_blob(data, overwrite=True, content_type=content_type)
