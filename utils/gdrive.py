import urllib.request

def download_from_gdrive(file_id: str, output_path: str) -> None:
    """
    Download a public Google Drive file using urllib.

    Args:
        file_id (str): Google Drive file ID.
        output_path (str): Destination path to save the downloaded file.
    """
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        urllib.request.urlretrieve(url, output_path)
        print(f"✅ Downloaded to: {output_path}")
    except Exception as e:
        print(f"❌ Failed to download {file_id}: {e}")

