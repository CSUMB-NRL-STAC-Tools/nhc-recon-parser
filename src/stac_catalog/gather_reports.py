import sys
import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin

__version__ = '1.0'

def download_reports(url: str):
    """Downloads text file reports to the nhc_text_files directory
    
    The url webpage containing the text file links shall be provided.
    """    
    download_directory = "nhc_text_files"

    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
        print(f"Created directory: {download_directory}")

    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        text_file_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.txt'):
                full_url = urljoin(url, href)
                text_file_links.append(full_url)

        if not text_file_links:
            print("No text files found on the page.")
        else:
            print(f"Found {len(text_file_links)} text files. Starting download...")
            for file_url in text_file_links:
                file_name = os.path.join(download_directory, os.path.basename(file_url))
                try:
                    file_response = requests.get(file_url, stream=True)
                    file_response.raise_for_status()

                    with open(file_name, 'wb') as f:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Downloaded: {os.path.basename(file_url)}")
                except requests.exceptions.RequestException as e:
                    print(f"Error downloading {os.path.basename(file_url)}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"Error accessing the URL {url}: {e}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python gather_reports.py <URL>")
        sys.exit(1)
    download_reports(sys.argv[1])