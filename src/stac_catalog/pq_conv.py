import polars as pl
import json
import io
import re
from datetime import datetime, timezone

# --- Import the parsing functions from main.py ---
from main import (
    decode_wind,
    decode_temp_dewpoint,
    decode_pressure_height,
    parse_temp_drop
)

# 1. Simulate Raw Dropsonde Report Content
simulated_raw_dropsonde_report_text = """
USNT13 KNMC 101603
879
XXAA 10161 99250 180000 00050
68165 99299 78813 88291 99919 29456 26508 00165 27850 27009
32851 21604 28009 85582 17440 30012 70214 07833 27510 58592 06757
21004 40763 167// 88999 77999
31313 89600 81539
61616 AF309 WXWXA 250710143501309 OB QD3
62626 MBL WND 27010 AEV 40001 DLM WND 26508 018400 WL150 27009 08
1 REL 2899N09125N 153947 SP4 2899N09123N 154849 =
XXBB 10168 99250 180000 00050
68168 99290 78819 88291 00019 29456 11929 21605 22892 25020
33850 17440 44780 12423 55711 09699 66699 97821 77688 00850 88644
00868 99632 03460 11848 82850 15558 24538 31578 00837 34504 01546
25552 00468 93545 32956 71239 02969 88537 02969 99999 00166 11808
07759 22483 08350 33469 09759 44462 10557 55456 11161 66441 12957
77430 14163 88416 15558
21212 00019 26509 11946 27511 22905 29508 33850 30812 44730 29011
55665 25012 66582 23510 77564 20010
31313 89600 81539
61616 AF309 WXWXA 250710143501309 OB QD3
62626 MBL WND 27010 AEV 40001 DLM WND 26508 018400 WL150 27009 08
1 REL 2899N09125N 153947 SP4 2899N09123N 154849 =
;
"""

# Call the imported parse_temp_drop function
parsed_report_data = parse_temp_drop(simulated_raw_dropsonde_report_text)

# --- Prepare data for Polars DataFrame ---
# The parse_temp_drop function returns a structured dictionary.
# We need to flatten this structure to create a tabular DataFrame.
# We'll prioritize the 'part_a_mandatory_levels' and 'part_b_significant_wind'
# as the primary observations, and merge header/remark data into each row.

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
    output_parquet_file = "dropsonde_observations_parsed_with_main_py.parquet"
    df.write_parquet(output_parquet_file)

    print(f"\nSuccessfully converted parsed dropsonde data to '{output_parquet_file}' using Polars.")
    print(f"You can now read this file back using: pl.read_parquet('{output_parquet_file}')")

else:
    print("No valid dropsonde observations found to convert to Parquet.")