import os
import json
# from pypgstac.db import PgstacDB
from pystac import Collection, Extent, SpatialExtent, TemporalExtent
import requests

# load_dotenv()  # Load environment variables from .env file

# conn_str = f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
def add_item_to_collection(stac_item: "pystac.Item", collection_id: str, api_base_url: str):
    """
    Adds a STAC item to the catalog

    Raises:
        requests.exceptions.InvalidURL: collection with requested id doesnt exist
        requests.exceptions.HTTPError: STAC item post request failed
    """    
    session = requests.Session()
    if collection_existance_check(collection_id, api_base_url):
        res = session.post(f'{api_base_url}/collections/{collection_id}/items',json=stac_item.to_dict())
        res.raise_for_status()
        return res.reason
    else:
        raise requests.exceptions.InvalidURL(f'Collection with id "{collection_id}" doesn\'t exist!')


def collection_existance_check(collection_id: str, api_base_url: str):
    session = requests.Session()
    res = session.get(f'{api_base_url}/collections/{collection_id}')
    if res.status_code == 200:
        return True
    
    return False