"""
Parses NHC Aircraft Reconnaissance data TEMP DROP text files into STAC items.

.. seealso:: :py:mod:`gather_reports`, :py:mod:`map_gen`
.. notes:: TEMP DROP message text files to parse must exist in the nhc_text_files 

.. author:: Zachary Davis <zdavis@csumb.edu>

.. changelog::
    .. versionadded:: 1.0
        Initial release of the module with core functionalities.
"""
from asyncio import gather
import os
import re
import json
from datetime import datetime, timezone
from turtle import st
from venv import create
import pystac
import requests
from . import gather_reports
from . import parser
__version__ = '1.0'

def verify_collection_existance(collection_url, collection_id, api_url):
    resp = requests.get(collection_url)
    if resp.status_code == 404:
        # Collection doesn't exist, create it
        collection = pystac.Collection(
            id=collection_id,
            description="Collection of dropsonde observations",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
                temporal=pystac.TemporalExtent([[datetime(2020, 1, 1), None]])
            ),
            license="CC-BY-4.0"
        )
        collection_dict = collection.to_dict()  # created using pystac.Collection
        create_resp = requests.post(f"{api_url}/collections", json=collection_dict)
        if create_resp.status_code not in (200, 201):
            raise Exception(f"Failed to create collection: {create_resp.text}")
    elif resp.status_code != 200:
        raise RuntimeError(f"Failed to check collection: {resp.text}")

def main():
    # """Parses TEMP DROP messages, verifys coordinates, and generates STAC items"""    
    # # Ensure 'parsed_reports' and 'stac_items' directories exist
    # os.makedirs('parsed_reports', exist_ok=True)
    # os.makedirs('stac_items', exist_ok=True)
    
    # rep_list = os.listdir('nhc_text_files')
    # for file in rep_list:
    #     with open(os.path.join('nhc_text_files', file), 'r', encoding='utf-8') as temp_drop_file:
    #         message = temp_drop_file.read()
    #     res = parse_temp_drop(message)
    #     with open(os.path.join('parsed_reports', f'{os.path.splitext(file)[0]}.json'), 'w') as result:
    #         json.dump(res, result, indent=4) 
    
    # if verify_lat_longs('parsed_reports'):
    #     print('All report coordinates matched!')

    # collection_id = "dropsondes"
    # api_url = "https://3.148.249.140"
    # collection_url = f"{api_url}/collections/{collection_id}"

    # # /parse?url=<link>

    # verify_collection_existance(collection_url, collection_id, api_url)
    
    # for file in rep_list:
    #     with open(os.path.join('parsed_reports', f'{os.path.splitext(file)[0]}.json'), 'r') as temp_drop_file:
    #         report = json.load(temp_drop_file)
    #         stac_item = convert_dropsonde_to_stac_item(report, os.path.join('nhc_text_files', file))

    #         # Write stac item to a file
    #         with open(os.path.join('stac_items', f'{os.path.splitext(file)[0]}_stac.json'), 'w') as stac_spec_file:
    #             json.dump(stac_item.to_dict(), stac_spec_file, indent=2)

    #         # Post stac item to catalog
    #         item_url = f"{api_url}/collections/{collection_id}/items"
    #         item_resp = requests.post(item_url, json=stac_item.to_dict())

    #         if item_resp.status_code not in (200, 201):
    #             print(f"Failed to post item: {item_resp.text}")
    #         else:
    #             print(f"Successfully posted item {item_resp.id} to collection '{collection_id}'.")
    pass

if __name__ == '__main__':
    main()
