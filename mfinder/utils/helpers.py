from typing import Union
import base64
from struct import pack
from pyrogram import raw
from pyrogram.file_id import FileId, FileType, PHOTO_TYPES, DOCUMENT_TYPES
import os
from dotenv import load_dotenv
import requests

load_dotenv()

def get_input_file_from_file_id(
    file_id: str,
    expected_file_type: FileType = None,
) -> Union["raw.types.InputPhoto", "raw.types.InputDocument"]:
    try:
        decoded = FileId.decode(file_id)
    except Exception:
        raise ValueError(
            f'Failed to decode "{file_id}". The value does not represent an existing local file, '
            f"HTTP URL, or valid file id."
        )

    file_type = decoded.file_type

    if expected_file_type is not None and file_type != expected_file_type:
        raise ValueError(
            f'Expected: "{expected_file_type}", got "{file_type}" file_id instead'
        )

    if file_type in (FileType.THUMBNAIL, FileType.CHAT_PHOTO):
        raise ValueError(f"This file_id can only be used for download: {file_id}")

    if file_type in PHOTO_TYPES:
        return raw.types.InputPhoto(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
        )

    if file_type in DOCUMENT_TYPES:
        return raw.types.InputDocument(
            id=decoded.media_id,
            access_hash=decoded.access_hash,
            file_reference=decoded.file_reference,
        )

    raise ValueError(f"Unknown file id: {file_id}")

def encode_file_id(s: bytes) -> str:
    r = b""
    n = 0

    for i in s + bytes([22]) + bytes([4]):
        if i == 0:
            n += 1
        else:
            if n:
                r += b"\x00" + bytes([n])
                n = 0

            r += bytes([i])

    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def encode_file_ref(file_ref: bytes) -> str:
    return base64.urlsafe_b64encode(file_ref).decode().rstrip("=")

def unpack_new_file_id(new_file_id):
    """Return file_id, file_ref"""
    decoded = FileId.decode(new_file_id)
    file_id = encode_file_id(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash,
        )
    )
    file_ref = encode_file_ref(decoded.file_reference)
    return file_id, file_ref

def edit_caption(c_caption):
    return c_caption

import os
import requests

def shorten_url(long_url):
    """
    Shortens a given URL using the Krown Links API and ensures SSL verification.
    
    Parameters:
        long_url (str): The URL to shorten.

    Returns:
        str: The shortened URL.

    Raises:
        Exception: If there is an error with the API or SSL verification.
    """
    api_url = "https://krownlinks.com/api"
    api_key = os.getenv("KROWN_API_KEY")
    
    if not api_key:
        raise Exception("KROWN_API_KEY is not set in .env file")
    
    params = {
        'url': long_url,
        'api': api_key,
        'format': 'json'
    }
    
    try:
        response = requests.get(api_url, params=params, verify=True)  # Ensure SSL verification
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success':
                return data['shortenedUrl']
            else:
                raise Exception(f"Error shortening URL: {data['message']}")
        else:
            raise Exception(f"HTTP Error: {response.status_code}")
    except requests.exceptions.SSLError as ssl_error:
        raise Exception(f"SSL verification failed: {ssl_error}")
    except Exception as e:
        raise Exception(f"An error occurred while shortening the URL: {e}")
