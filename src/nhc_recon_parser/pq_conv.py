import polars as pl
import json
import io
import re
from datetime import datetime, timezone
import sys

from nhc_recon_parser.parser import parse_temp_drop
from nhc_recon_parser.gather_reports import read_dropsonde_message

def main():
    # 1. Read Raw Dropsonde Report Content from File
    if len(sys.argv) < 2:
        print("Usage: python pq_conv.py <path_to_raw_dropsonde_report_text_file>")
        sys.exit(1)

    input_file_path = sys.argv[1]

    try:
    # Call the imported parse_temp_drop function
        parsed_report_data = parse_temp_drop(*read_dropsonde_message(input_file_path))
    except FileNotFoundError:
        print(f"Error: The file '{input_file_path}' was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing file '{input_file_path}': {e}")
        sys.exit(1)

    # --- Prepare data for Polars DataFrame ---

    all_parsed_rows = []

    # Get header and remarks that apply to all observations
    common_header = parsed_report_data.get("header", {})
    common_remarks = parsed_report_data.get("remarks", {})
    sounding_system_info = parsed_report_data.get("part_a_sounding_system", {})

    # Iterate through Part A Mandatory Levels
    for obs_a in parsed_report_data.get("part_a_mandatory_levels", []):
        row = {
            "level_type": "mandatory_level_A",
            **obs_a, # Add the specific observation data
            **common_header, # Add all header data
            **common_remarks, # Add all remarks
            **sounding_system_info # Add sounding system info
        }
        all_parsed_rows.append(row)

    # Iterate through Part B Significant Temperature and Humidity Levels
    for obs_b_temp_hum in parsed_report_data.get("part_b_significant_temp_humidity", []):
        row = {
            "level_type": "significant_temp_humidity_B",
            **obs_b_temp_hum,
            **common_header,
            **common_remarks,
            **sounding_system_info
        }
        all_parsed_rows.append(row)

    # Iterate through Part B Significant Wind Levels
    for obs_b_wind in parsed_report_data.get("part_b_significant_wind", []):
        row = {
            "level_type": "significant_wind_B",
            **obs_b_wind,
            **common_header,
            **common_remarks,
            **sounding_system_info
        }
        all_parsed_rows.append(row)

    # Add Tropopause and Max Wind as separate rows if they exist and are not 'not_observed'
    if parsed_report_data.get("part_a_tropopause") and not parsed_report_data["part_a_tropopause"].get("not_observed"):
        row = {
            "level_type": "tropopause_A",
            **parsed_report_data["part_a_tropopause"],
            **common_header,
            **common_remarks,
            **sounding_system_info
        }
        all_parsed_rows.append(row)

    if parsed_report_data.get("part_a_max_wind") and not parsed_report_data["part_a_max_wind"].get("not_observed"):
        row = {
            "level_type": "max_wind_A",
            **parsed_report_data["part_a_max_wind"],
            **common_header,
            **common_remarks,
            **sounding_system_info
        }
        all_parsed_rows.append(row)


    # Create Polars DataFrame
    if all_parsed_rows:
        df = pl.DataFrame(all_parsed_rows)

        print("\nDataFrame before writing to Parquet:")
        print(df)

        # --- Write to Parquet file using Polars ---
        output_parquet_file = "dropsonde_observations.parquet"
        df.write_parquet(output_parquet_file)
        print(f"\nSuccessfully converted data to Parquet: {output_parquet_file}")

    else:
        print("No valid dropsonde observations found to convert to Parquet.")

if __name__ == '__main__':
    main()