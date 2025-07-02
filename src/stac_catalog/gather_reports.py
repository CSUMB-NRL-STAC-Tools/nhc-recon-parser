"""
Downloads text file reports from a specified url

.. author:: Zachary Davis <zdavis@csumb.edu>

.. changelog::
    .. versionadded:: 1.0
        Initial release of the module with core functionalities.
"""
import sys
import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin # Corrected import
__version__ = '1.0'
def main():
    """
    Downloads text file reports to the nhc_text_files directory
    
    The url webpage containing the text file links shall be provided as a
    command line argument.
    """    
    # url = "https://www.nhc.noaa.gov/archive/recon/2025/REPNT3/"
    url = sys.argv[1]
    download_directory = "nhc_text_files"

    # Create the download directory if it doesn't exist
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)
        print(f"Created directory: {download_directory}")

    try:
        # Fetch the content of the index page
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find all links that end with '.txt'
        text_file_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.txt'):
                # Construct the full URL if the link is relative
                full_url = urljoin(url, href) # Corrected usage
                text_file_links.append(full_url)

        if not text_file_links:
            print("No text files found on the page.")
        else:
            print(f"Found {len(text_file_links)} text files. Starting download...")
            # Download each text file
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

def get_file_content(file_url):
    """
    Downloads and returns the content of a text file from the specified URL.

    :param file_url: The URL of the text file to download.
    :type file_url: str
    :return: The content of the file as a string.
    :rtype: str
    :raises requests.exceptions.RequestException: If the HTTP request fails.
    """
    response = requests.get(file_url)
    response.raise_for_status()
    return response.text

def iter_urls_from_archive_page(archive_url):
    """
    Lazily yields text file URLs from the specified archive page.

    :param archive_url: URL of the archive page containing text file links.
    :yield: Full URLs to the text files, one at a time.
    """
    response = requests.get(archive_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')

    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.txt'):
            yield urljoin(archive_url, href)

if __name__ == '__main__':
    main()
