from nhc_recon_parser import gather_reports, parser, api_util
import json
import os
import argparse
from urllib.parse import urlparse
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def main():
    # 1. Set up argument parsing
    cli_parser = argparse.ArgumentParser(
        description='Process NHC dropsonde data from a URL, a local file, or an archive URL.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    cli_parser.add_argument(
        '--url',
        type=str,
        help='URL to fetch a SINGLE dropsonde data file.\nExample: https://www.nhc.noaa.gov/archive/recon/2024/REPNT3/REPNT3-KNHC.202401232347.txt'
    )
    cli_parser.add_argument(
        '--archive_url',
        type=str,
        help='URL to an NHC recon archive page containing multiple .txt file links.\nExample: https://www.nhc.noaa.gov/archive/recon/2025/REPNT0/'
    )
    cli_parser.add_argument(
        '--local_file',
        type=str,
        help='Path to a SINGLE local dropsonde .txt file.\nExample: REPNT3-KNHC.202405051723.txt'
    )
    cli_parser.add_argument(
        '--api_base_url',
        type=str,
        default='http://54.193.30.87',
        help='Base URL for the STAC API to upload items to. Default: http://54.193.30.87'
    )
    cli_parser.add_argument(
        '--collection',
        type=str,
        default='dropsonde',
        help='STAC Collection ID to upload items to. Default: dropsonde'
    )
    cli_parser.add_argument(
        '--output_dir',
        type=str,
        default='stac_items_output', # Default output directory
        help='Directory to save generated STAC JSON files. Will be created if it does not exist. Default: stac_items_output'
    )

    args = cli_parser.parse_args()

    processed_any_input = False

    # Create output directory if it doesn't exist
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        print(f"Created output directory: {args.output_dir}")

    # Function to process and save a single dropsonde message
    def process_and_save_dropsonde(source_path, is_local=False, stats_tracker=None):
        nonlocal processed_any_input
        processed_any_input = True
        source_type = "Local File" if is_local else "URL"

        
        if stats_tracker:
            stats_tracker['attempted'] += 1

        print(f"\n--- Processing from {source_type}: {source_path} ---")
        try:
            dropsonde_message_content = gather_reports.read_dropsonde_message(source_path)
            dropsonde_report = parser.parse_temp_drop(*dropsonde_message_content)
            stac_item = parser.convert_dropsonde_to_stac_item(dropsonde_report)

            # Generate unique filename for STAC item
            sanitized_id = re.sub(r'[^\w\d\-\.]', '_', stac_item.id)
            output_filename_json = os.path.join(args.output_dir, f"{sanitized_id}.json")
            output_filename_parquet = os.path.join(args.output_dir, f"{sanitized_id}.parquet")

            print(f"Parsed STAC Item (ID: {stac_item.id}):\n{json.dumps(stac_item.to_dict(), indent=4)}")

            try:
                # Attempt to add item to collection
                print(api_util.add_item_to_collection(stac_item, args.collection, args.api_base_url))
                if stats_tracker:
                    stats_tracker['successful'] += 1
            except Exception as e:
                print(f"Error adding STAC item to collection for {source_type} ({source_path}): {e}")

            # Save as JSON file
            with open(output_filename_json, 'w') as f:
                json.dump(stac_item.to_dict(), f, indent=4)
            print(f"STAC item saved to: {output_filename_json}")

            # Extract properties and convert to a pandas DataFrame
            item_data = stac_item.to_dict()
            properties = item_data.get('properties', {})
            
            # Create a DataFrame from the properties
            df = pd.DataFrame([properties])

            df['id'] = item_data.get('id')
            df['geometry'] = str(item_data.get('geometry'))
            
            # Save the DataFrame to a Parquet file
            df.to_parquet(output_filename_parquet)
            print(f"STAC item saved to: {output_filename_parquet}")

        except FileNotFoundError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"An error occurred during {source_type} processing of {source_path}: {e}")

    # 2. Process from Archive URL if provided
    if args.archive_url:
        processed_any_input = True
        print(f"Attempting to iterate URLs from archive page: {args.archive_url}")
        
        archive_stats = {'successful': 0, 'attempted': 0}

        try:
            for url in gather_reports.iter_urls_from_archive_page(args.archive_url):
                process_and_save_dropsonde(url, stats_tracker=archive_stats)
        except Exception as e:
            print(f"An error occurred during archive URL processing: {e}")
        finally:
            print("\n--- Archive Processing Summary ---")
            print(f"Attempted to process: {archive_stats['attempted']} files")
            print(f"Successfully uploaded to STAC server: {archive_stats['successful']} files")
            print("----------------------------------")

    # 3. Process from a Single URL if provided
    elif args.url:
        process_and_save_dropsonde(args.url)

    # 4. Process from a Single Local File if provided
    elif args.local_file:
        if os.path.exists(args.local_file):
            process_and_save_dropsonde(args.local_file, is_local=True)
        else:
            processed_any_input = True # Mark as processed to avoid "No input" message
            print(f"Error: Local file not found: {args.local_file}. Please ensure this file exists or provide a full path.")

    # 5. Handle no arguments provided
    if not processed_any_input:
        print("\nNo URL, archive URL, or local file path provided.")
        print("Please use --url <URL>, --archive_url <URL>, or --local_file <PATH>.")
        cli_parser.print_help()

if __name__ == "__main__":
    main()
