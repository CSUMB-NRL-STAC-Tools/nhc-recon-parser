import os
import json
# from pypgstac.db import PgstacDB
from pystac import Collection, Extent, SpatialExtent, TemporalExtent
import requests

CREATE_COLLECTION = True  # Set to False if you want to skip collection creation if it doesnt exist
# load_dotenv()  # Load environment variables from .env file

def create_dropsonde_collection(collection_id: str, api_base_url: str):
    collection_description = "Collection of dropsonde observations from NHC Recon flights"
    collection_title = "Dropsonde Observations"

    # Define initial extent (will be updated by PgSTAC if enabled)
    initial_extent = Extent(
        spatial=SpatialExtent(bboxes=[[-180.0, -90.0, 180.0, 90.0]]), # Global Extent (WGS84)
        temporal=TemporalExtent(intervals=[None, None]) # Dont define a time range
    )

    stac_collection = Collection(
        id=collection_id,
        description=collection_description,
        title=collection_title,
        extent=initial_extent,
        license="CC-BY-4.0"
    )
    session = requests.Session()
    res = session.post(f'{api_base_url}/collections',json=stac_collection.to_dict())
    res.raise_for_status()
    print(f"Collection '{collection_id}' created successfully.")
    return res.reason

# conn_str = f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
def add_item_to_collection(stac_item: "pystac.Item", collection_id: str, api_base_url: str):
    """
    Adds a STAC item to the catalog

    Raises:
        requests.exceptions.InvalidURL: collection with requested id doesnt exist
        requests.exceptions.HTTPError: STAC item post request failed
    """    
    session = requests.Session()
    col_exists = collection_existance_check(collection_id, api_base_url)
    if col_exists or CREATE_COLLECTION:
        if not col_exists:
            create_dropsonde_collection(collection_id, api_base_url)
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
    
    print('Collection with id "%s" does not exist!' % collection_id)
    return False