import os
import requests
from typing import Dict

class SlackAPIError(RuntimeError):
    """High-level error in Slack Web API calls."""

def get_upload_url_external(token: str, filename: str, length: int, timeout: int = 30) -> Dict[str, str]:
    """
    Request a pre-signed upload URL from Slack (files.getUploadURLExternal).

    Args:
        token (str): Slack OAuth token with the required scope (files:write).
        filename (str): File name (without directory path) to associate with the upload.
        length (int): File size in bytes.
        timeout (int, optional): HTTP request timeout in seconds. Defaults to 30.

    Returns:
        Dict[str, str]: A dictionary containing:
            - "upload_url": Pre-signed URL to upload the file content via HTTP PUT.
            - "file_id": Slack file ID for subsequent API calls.
    """
    if not token:
        raise ValueError("Token Slack ausente.")

    resp = requests.get(
        "https://slack.com/api/files.getUploadURLExternal",
        headers={"Authorization": f"Bearer {token}"},
        params={"filename": filename, "length": length},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise SlackAPIError(f"Slack getUploadURLExternal falhou: {data}")
    return {"upload_url": data["upload_url"], "file_id": data["file_id"]}

def upload_to_presigned_url(upload_url: str, local_path: str, filename: str, timeout: int = 120) -> None:
    """
    Upload a local file to Slack’s pre-signed URL (step 2 of the external upload flow).

    Args:
        upload_url (str): The pre-signed URL returned by `files.getUploadURLExternal`.
        local_path (str): Path to the local file to upload.
        filename (str): File name to associate with the upload (form field value).
        timeout (int, optional): HTTP request timeout in seconds. Defaults to 120.

    Returns:
        None
    """
    if not upload_url or not filename:
        raise ValueError("Parâmetros inválidos para upload (upload_url/filename).")

    with open(local_path, "rb") as fh:
        resp = requests.post(
            upload_url,
            files={"filename": (filename, fh, "application/pdf")},
            timeout=timeout,
        )
    if resp.status_code != 200:
        raise SlackAPIError(
            f"Falha no upload para URL pré-assinada. status={resp.status_code} body={resp.text[:500]}"
        )

def complete_upload_external(
    token: str,
    external_file_id: str,
    channel_id: str,
    title: str,
    initial_comment: str,
    timeout: int = 30,
) -> None:
    """
    Finalize Slack’s external file upload and share the file in a channel.

    Args:
        token (str): Slack OAuth token with required scopes (files:write, chat:write).
        external_file_id (str): The file ID returned by `files.getUploadURLExternal`.
        channel_id (str): Destination channel ID where the file will be shared.
        title (str): Display title to associate with the uploaded file.
        initial_comment (str): Optional message to accompany the shared file.
        timeout (int, optional): HTTP request timeout in seconds. Defaults to 30.

    Returns:
        None
    """
    if not token:
        raise ValueError("Token Slack ausente.")
    if not external_file_id or not channel_id:
        raise ValueError("Parâmetros inválidos (external_file_id/channel_id).")

    resp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={"Authorization": f"Bearer {token}"},
        data={
            "files": f'[{{"id":"{external_file_id}","title":"{title}"}}]',
            "channel_id": channel_id,
            "initial_comment": initial_comment,
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise SlackAPIError(f"Slack completeUploadExternal falhou: {data}")
