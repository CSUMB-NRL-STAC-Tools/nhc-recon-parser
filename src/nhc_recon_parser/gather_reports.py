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
from urllib.parse import urljoin, urlparse
__version__ = '1.0'

def read_dropsonde_message(path):
    """
    Reads content from a given path, which can be either a local file path
    or a web URL.

    Args:
        path (str): The path to the file or URL to read.

    Returns:
        tuple: content and filename

    Raises:
        FileNotFoundError: If the local file does not exist.
        requests.exceptions.RequestException: If there's an error fetching content from the URL.
        ValueError: If the path is invalid or cannot be processed.
    """
    try:
        parsed_url = urlparse(path)
        # Check if the path is a URL (has a scheme like http, https, ftp)
        if parsed_url.scheme in ('http', 'https', 'ftp', 'ftps'):
            print(f"Attempting to read content from URL: {path}")
            response = requests.get(path)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            return (response.text, requests.urllib3.util.parse_url(path).path.split('/')[-1])
        else:
            # Assume it's a local file path
            print(f"Attempting to read content from local file: {path}")
            if not os.path.exists(path):
                raise FileNotFoundError(f"Local file not found: {path}")
            with open(path, 'r', encoding='utf-8') as f:
                return (f.read(), os.path.basename(path))
    except FileNotFoundError as e:
        print(f"Error: {e}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Error fetching content from URL: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise ValueError(f"Could not process path '{path}': {e}")

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
