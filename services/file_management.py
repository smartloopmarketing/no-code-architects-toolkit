# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



import os
import uuid
import requests
from urllib.parse import urlparse
import mimetypes
import boto3
import shutil

def get_extension_from_url(url):
    """Extract file extension from URL or content type.
    
    Args:
        url (str): The URL to extract the extension from
        
    Returns:
        str: The file extension including the dot (e.g., '.jpg')
        
    Raises:
        ValueError: If no valid extension can be determined from the URL or content type
    """
    # First try to get extension from URL
    parsed_url = urlparse(url)
    path = parsed_url.path
    if path:
        ext = os.path.splitext(path)[1].lower()
        if ext:
            return ext

    # If no extension in URL, try to determine from content type
    try:
        response = requests.head(url, allow_redirects=True)
        content_type = response.headers.get('content-type', '').split(';')[0]
        ext = mimetypes.guess_extension(content_type)
        if ext:
            return ext.lower()
    except:
        pass

    # If we can't determine the extension, raise an error
    raise ValueError(f"Could not determine file extension from URL: {url}")

def _parse_s3_bucket_key(media_url: str):
    """Return (bucket, key) if URL is an S3 URL we can parse; else (None, None)."""
    parsed = urlparse(media_url)
    host = parsed.netloc
    path = parsed.path.lstrip('/')

    # Virtual-hosted-style: bucket.s3.region.amazonaws.com/key
    if host.endswith('.amazonaws.com') and '.s3.' in host and not host.startswith('s3.'):
        bucket = host.split('.')[0]
        key = path
        return bucket, key

    # Path-style: s3.region.amazonaws.com/bucket/key
    if host.startswith('s3.') and host.endswith('.amazonaws.com'):
        parts = path.split('/', 1)
        if len(parts) == 2:
            return parts[0], parts[1]
    return None, None

def _download_from_s3_with_credentials(media_url: str, destination_path: str) -> bool:
    """Attempt to download using S3 credentials if available. Returns True on success."""
    bucket, key = _parse_s3_bucket_key(media_url)
    if not bucket or not key:
        return False

    # Prefer project S3_* envs; fallback to standard AWS_* envs
    endpoint_url = os.environ.get('S3_ENDPOINT_URL')
    region_name = os.environ.get('S3_REGION') or None
    access_key = os.environ.get('S3_ACCESS_KEY') or os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('S3_SECRET_KEY') or os.environ.get('AWS_SECRET_ACCESS_KEY')

    if not access_key or not secret_key:
        return False

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )
    s3 = session.client('s3', endpoint_url=endpoint_url)

    # Use GetObject streaming to avoid implicit HeadObject permission requirement
    response = s3.get_object(Bucket=bucket, Key=key)
    body = response.get('Body')
    if body is None:
        return False
    with open(destination_path, 'wb') as fh:
        shutil.copyfileobj(body, fh)
    return True

def download_file(url, storage_path="/tmp/"):
    """Download a file from URL to local storage.

    - If the URL is public (HTTP 200), stream via requests.
    - If it's an S3 URL and public access is denied (e.g., 403), attempt to
      download using configured S3 credentials.
    """
    os.makedirs(storage_path, exist_ok=True)

    file_id = str(uuid.uuid4())
    extension = get_extension_from_url(url)
    local_filename = os.path.join(storage_path, f"{file_id}{extension}")

    try:
        response = requests.get(url, stream=True)
        if response.status_code == 403:
            # Try authenticated S3 download if possible
            if _download_from_s3_with_credentials(url, local_filename):
                return local_filename
        response.raise_for_status()

        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        return local_filename
    except Exception as e:
        if os.path.exists(local_filename):
            os.remove(local_filename)
        raise e

