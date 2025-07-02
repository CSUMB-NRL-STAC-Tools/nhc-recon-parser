import os
import json
from pypgstac.db import PgstacDB
from pystac import Collection, Extent, SpatialExtent, TemporalExtent
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

conn_str = f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
def add_item_to_catlog(stac_item: "pystac.Item"):
    """
    Adds a STAC item to the catalog, ensuring the collection exists.
    If the collection does not exist, it will be created.
    """
    # --- 1. Define your Collection Data ---
    # This is the collection you want to ensure exists or update.
    # You should always provide the full, current state of the collection.
    # For simplicity, we'll start with a basic one.
    # In a real application, you might load this from a file or generate it
    # based on your data.

    collection_id = "dropsonde"
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

    # --- 3. Establish Database Connection (using environment variables) ---
    # Make sure your PG* environment variables are set (PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD)

    # --- 4. Perform the Upsert Operations ---

    print(f"Connecting to database: {os.getenv('PGDATABASE')}@{os.getenv('PGHOST')}:{os.getenv('PGPORT')} as {os.getenv('PGUSER')}...")
    try:
        with PgstacDB(dsn=conn_str) as db:
            print(f"Connected to database successfully.")

            print(f"Attempting to ingest collection: {stac_collection.id}")
            # Check if the collection already exists
            existing = db.query_one(
                "SELECT id FROM pgstac.collections WHERE id = %s",
                (stac_collection.id,)
            )
            if existing is None:
                db.query_one(
                    "SELECT pgstac.create_collection(%s)",
                    (json.dumps(stac_collection.to_dict()),)
                )
                print(f"Collection '{stac_collection.id}' ingested successfully.")
            else:
                print(f"Collection '{stac_collection.id}' already exists. Skipping creation.")

            # Ingest the STAC Item by calling the pgstac.create_item function
            print(f"Attempting to ingest item: {stac_item.id}")
            db.query_one(
                "SELECT pgstac.create_item(%s)",
                (json.dumps(stac_item.to_dict()),)
            )
            print(f"Item '{stac_item.id}' for collection '{stac_item.collection_id}' ingested successfully.")

    except Exception as e:
        print(f"An error occurred during database operation: {e}")
        import traceback
        traceback.print_exc()

    print("Ingestion script finished.")