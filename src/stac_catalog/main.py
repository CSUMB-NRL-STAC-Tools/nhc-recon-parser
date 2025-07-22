import os
import re
import json
from datetime import datetime, timezone
import pystac
import sys
import gather_reports as gr
import map_gen as mg


def decode_wind(wind_group_str: str):
    if len(wind_group_str) != 5 or not wind_group_str.isdigit():
        return None, None
    d_hundreds_tens = int(wind_group_str[0:2])
    f_hundreds_plus_d_unit = int(wind_group_str[2])
    f_tens_units = int(wind_group_str[3:5])
    if f_hundreds_plus_d_unit >= 5:
        d_unit = 5
        f_hundreds = f_hundreds_plus_d_unit - 5
    else:
        d_unit = 0
        f_hundreds = f_hundreds_plus_d_unit
    direction = d_hundreds_tens * 10 + d_unit
    speed = f_hundreds * 100 + f_tens_units
    return direction, speed

def decode_temp_dewpoint(temp_dew_group_str: str):
    if len(temp_dew_group_str) != 5 or not temp_dew_group_str.isdigit():
        return None, None
    temperature = float(temp_dew_group_str[0:3]) / 10.0
    dew_point_depression = int(temp_dew_group_str[3:5])
    return temperature, dew_point_depression

def decode_pressure_height(pressure_height_group_str: str):
    if len(pressure_height_group_str) != 5 or not pressure_height_group_str.isdigit():
        return None, None
    pressure = int(pressure_height_group_str[0:3])
    height = int(pressure_height_group_str[3:5]) * 10
    return pressure, height

def parse_temp_drop(message: str):
    lines = [line.strip() for line in message.split('\n') if line.strip()]
    parsed_data = {
        "header": {},
        "part_a_mandatory_levels": [],
        "part_a_tropopause": None,
        "part_a_max_wind": None,
        "part_a_sounding_system": None,
        "part_b_significant_temp_humidity": [],
        "part_b_significant_wind": [],
        "remarks": {}
    }
    part_a_active = False # Keep track of which part's data we are currently parsing
    part_b_active = False

    for i, line in enumerate(lines):
        if not line:
            continue
        
        # Original logic for WMO header parsing at i == 0 (e.g., "USNT13 KNHC 202100")
        if i == 0:
            parts = line.split()
            if len(parts) >= 3:
                parsed_data["header"] = {
                    "wmo_header": parts[0],
                    "icao_originator": parts[1],
                    "transmission_date_time_group": parts[2]
                }
                if parts[0].startswith("URPN"): 
                    pass 
            continue

        # Logic to handle XXAA and XXBB lines dynamically
        if line.startswith("XXAA") or line.startswith("XXBB"):
            parts = line.split()
            if len(parts) >= 5 and parts[1].isdigit() and len(parts[1]) == 5:
                identifier = parts[0]
                day_hour_id_indicator = parts[1]
                lat_str = parts[2]
                lon_str = parts[3]
                marsden_str = parts[4]

                if identifier.startswith("XXBB"):
                    part_b_active = True
                    part_a_active = False
                    prefix = "part_b_"
                else:
                    part_a_active = True
                    part_b_active = False
                    prefix = "part_a_"

                try:
                    parsed_data["header"][f"{prefix}identifier"] = identifier
                    parsed_data["header"][f"{prefix}day"] = int(day_hour_id_indicator[0:2])
                    parsed_data["header"][f"{prefix}hour"] = int(day_hour_id_indicator[2:4])
                    parsed_data["header"][f"{prefix}id_indicator"] = int(day_hour_id_indicator[4])
                    
                    parsed_data["header"][f"{prefix}latitude"] = float(lat_str[2:]) / 10.0
                    
                    quadrant = int(lon_str[0])
                    parsed_data["header"][f"{prefix}quadrant"] = quadrant
                    parsed_data["header"][f"{prefix}longitude"] = float(lon_str[1:]) / 10.0

                    if quadrant in [3, 5]:
                        parsed_data["header"][f"{prefix}latitude"] *= -1
                    if quadrant in [5, 7]:
                        parsed_data["header"][f"{prefix}longitude"] *= -1

                    parsed_data["header"][f"{prefix}marsden_square"] = int(marsden_str[0:3])
                    parsed_data["header"][f"{prefix}ula"] = int(marsden_str[3])
                    parsed_data["header"][f"{prefix}ulo"] = int(marsden_str[4])

                except ValueError as e:
                    print(f"Warning: Error parsing {identifier} line '{line}': {e}. Skipping details for this section.")
            continue

        # NEW LOGIC: Handle "B." line for VORTEX DATA MESSAGE (URPN/REPPN2 files)
        elif line.startswith("B."):
            match = re.search(r'B.\s+(\d+\.\d+)\s+deg\s+([NS])\s+(\d+\.\d+)\s+deg\s+([EW])', line)
            if match:
                try:
                    lat_val = float(match.group(1))
                    lat_dir = match.group(2)
                    lon_val = float(match.group(3))
                    lon_dir = match.group(4)

                    # Convert to decimal degrees
                    latitude = lat_val if lat_dir == 'N' else -lat_val
                    longitude = lon_val if lon_dir == 'E' else -lon_val

                    # For Vortex Data Messages, this is the primary and often only location.
                    parsed_data["header"]["part_a_latitude"] = latitude
                    parsed_data["header"]["part_a_longitude"] = longitude
                    parsed_data["header"]["part_b_latitude"] = latitude
                    parsed_data["header"]["part_b_longitude"] = longitude
                    
                except ValueError as e:
                    print(f"Warning: Error parsing coordinates from 'B.' line '{line}': {e}. Skipping coordinates.")
            else:
                print(f"Warning: 'B.' line found but could not parse coordinates using regex: {line}")
            continue # Continue to next line after processing B.

        # Existing logic for 31313, 61616, 62626, mandatory levels etc.

        if line.startswith("31313"):
            parts = line.split()
            if len(parts) >= 3:
                try:
                    srrarasasa = parts[1]
                    launch_time_group = parts[2]
                    if part_a_active:
                        parsed_data["part_a_sounding_system"] = {
                            "sounding_system_indicator_raw": srrarasasa,
                            "solar_ir_correction": int(srrarasasa[0]),
                            "radiosonde_system_used": int(srrarasasa[1:3]),
                            "tracking_technique_status": int(srrarasasa[3:5]),
                            "launch_time_indicator": int(launch_time_group[0]),
                            "launch_hour_utc": int(launch_time_group[1:3]),
                            "launch_minute_utc": int(launch_time_group[3:5])
                        }
                    elif part_b_active:
                         parsed_data["part_b_sounding_system"] = {
                            "sounding_system_indicator_raw": srrarasasa,
                            "solar_ir_correction": int(srrarasasa[0]),
                            "radiosonde_system_used": int(srrarasasa[1:3]),
                            "tracking_technique_status": int(srrarasasa[3:5]),
                            "launch_time_indicator": int(launch_time_group[0]),
                            "launch_hour_utc": int(launch_time_group[1:3]),
                            "launch_minute_utc": int(launch_time_group[3:5])
                        }
                    else: # Handle 31313 for non-XXAA/XXBB reports like URPN
                        # If no active part, assign to a general sounding_system or Part A if it's the primary system
                        parsed_data["sounding_system"] = { # Creating a general sounding system key
                            "sounding_system_indicator_raw": srrarasasa,
                            "solar_ir_correction": int(srrarasasa[0]),
                            "radiosonde_system_used": int(srrarasasa[1:3]),
                            "tracking_technique_status": int(srrarasasa[3:5]),
                            "launch_time_indicator": int(launch_time_group[0]),
                            "launch_hour_utc": int(launch_time_group[1:3]),
                            "launch_minute_utc": int(launch_time_group[3:5])
                        }
                except ValueError as e:
                    print(f"Warning: Error parsing Section 7 (31313) line: {e}. Skipping sounding system details.")
            continue
        if line.startswith("61616"):
            parsed_data["remarks"]["mission_info"] = line[6:].strip()
            continue
        if line.startswith("62626"):
            remark_string = line[6:].strip()
            remark_segments = re.split(r'(MBL WND|AEV|DLM WND|WL|REL|SPG|EYEWALL)', remark_string)
            
            current_key = "initial_description"
            for segment in remark_segments:
                segment = segment.strip()
                if not segment:
                    continue
                
                if segment in ["MBL WND", "AEV", "DLM WND", "WL", "REL", "SPG", "EYEWALL"]:
                    current_key = segment.replace(" ", "_").lower()
                else:
                    if current_key:
                        parsed_data["remarks"][current_key] = segment
                    current_key = None

            if "rel" in parsed_data["remarks"] and parsed_data["remarks"]["rel"]:
                rel_parts = parsed_data["remarks"]["rel"].split()
                if len(rel_parts) >= 3:
                    parsed_data["remarks"]["rel_location"] = rel_parts[0]
                    parsed_data["remarks"]["rel_time"] = rel_parts[1]
            if "spg" in parsed_data["remarks"] and parsed_data["remarks"]["spg"]:
                spg_parts = parsed_data["remarks"]["spg"].split()
                if len(spg_parts) >= 3:
                    parsed_data["remarks"]["spg_location"] = spg_parts[0]
                    parsed_data["remarks"]["spg_time"] = spg_parts[1]
            continue
        
        if part_a_active: # This block is primarily for REPNT-style reports
            # Mandatory Levels (Part A)
            if re.match(r'^\d{5}\s+\d{5}\s+\d{5}(\s+\d{5}\s+\d{5}\s+\d{5})*$', line) or \
               re.match(r'^\d{3}\d{2}\s+\d{3}\d{2}\s+\d{5}(\s+\d{3}\d{2}\s+\d{3}\d{2}\s+\d{5})*$', line):
                groups = line.split()
                for j in range(0, len(groups), 3):
                    if j + 2 < len(groups):
                        try:
                            pressure, height = decode_pressure_height(groups[j])
                            temp, dew_point_depression = decode_temp_dewpoint(groups[j+1])
                            wind_dir, wind_speed = decode_wind(groups[j+2])
                            parsed_data["part_a_mandatory_levels"].append({
                                "pressure_mb": pressure,
                                "height_m": height,
                                "temperature_c": temp,
                                "dew_point_depression_c": dew_point_depression,
                                "wind_direction_deg": wind_dir,
                                "wind_speed_kt": wind_speed
                            })
                        except ValueError as e:
                            print(f"Warning: Error parsing Part A mandatory level group '{groups[j]} {groups[j+1]} {groups[j+2]}': {e}. Skipping this group.")
                continue
            # Tropopause (Part A)
            if line.startswith("88"):
                parts = line.split()
                if parts[0] == "88999":
                    parsed_data["part_a_tropopause"] = {"not_observed": True}
                elif len(parts) >= 3:
                    try:
                        pressure = int(parts[0][2:])
                        temp, dew_point_depression = decode_temp_dewpoint(parts[1])
                        wind_dir, wind_speed = decode_wind(parts[2])
                        parsed_data["part_a_tropopause"] = {
                            "pressure_mb": pressure,
                            "temperature_c": temp,
                            "dew_point_depression_c": dew_point_depression,
                            "wind_direction_deg": wind_dir,
                            "wind_speed_kt": wind_speed
                        }
                    except ValueError as e:
                        print(f"Warning: Error parsing Section 3 (Tropopause) line: {e}. Skipping tropopause details.")
                continue
            # Max Wind (Part A)
            if line.startswith("77") or line.startswith("66"):
                parts = line.split()
                if parts[0] == "77999":
                    parsed_data["part_a_max_wind"] = {"not_observed": True}
                elif len(parts) >= 2:
                    try:
                        pressure = int(parts[0][2:])
                        wind_dir, wind_speed = decode_wind(parts[1])
                        max_wind_data = {
                            "indicator": parts[0][0:2],
                            "pressure_mb": pressure,
                            "wind_direction_deg": wind_dir,
                            "wind_speed_kt": wind_speed
                        }
                        if len(parts) > 2 and parts[2].startswith("4"):
                            vbvb = int(parts[2][1:3])
                            vava = int(parts[2][3:5])
                            max_wind_data["vertical_wind_shear"] = {
                                "below_max_wind_kt": vbvb,
                                "above_max_wind_kt": vava
                            }
                        parsed_data["part_a_max_wind"] = max_wind_data
                    except ValueError as e:
                        print(f"Warning: Error parsing Section 4 (Max Wind) line: {e}. Skipping max wind details.")
                continue
        
        if part_b_active: # This block is primarily for REPNT-style reports
            # Significant Temperature and Humidity Levels (Part B)
            if re.match(r'^(\d{5}\s+){1}\d{5}(\s+\d{5}\s+\d{5})*$', line):
                groups = line.split()
                for j in range(0, len(groups), 2):
                    if j + 1 < len(groups):
                        try:
                            level_num = int(groups[j][0:2])
                            pressure = int(groups[j][2:5])
                            temp, dew_point_depression = decode_temp_dewpoint(groups[j+1])
                            parsed_data["part_b_significant_temp_humidity"].append({
                                "level_number": level_num,
                                "pressure_mb": pressure,
                                "temperature_c": temp,
                                "dew_point_depression_c": dew_point_depression
                            })
                        except ValueError as e:
                            print(f"Warning: Error parsing Part B significant temp/humidity group '{groups[j]} {groups[j+1]}': {e}. Skipping this group.")
                continue
            # Significant Wind Levels (Part B) - starts with "21212"
            if line.startswith("21212"):
                data_string = line[6:].strip()
                groups = data_string.split()
                for j in range(0, len(groups), 2):
                    if j + 1 < len(groups):
                        try:
                            level_num = int(groups[j][0:2])
                            pressure = int(groups[j][2:5])
                            wind_dir, wind_speed = decode_wind(groups[j+1])
                            parsed_data["part_b_significant_wind"].append({
                                "level_number": level_num,
                                "pressure_mb": pressure,
                                "wind_direction_deg": wind_dir,
                                "wind_speed_kt": wind_speed
                            })
                        except ValueError as e:
                            print(f"Warning: Error parsing Section 6 significant wind group '{groups[j]} {groups[j+1]}': {e}. Skipping this group.")
                continue
    
    # If one is present and the other is not, set them to be equal to allow verify_lat_longs to proceed.
    if "part_a_latitude" in parsed_data["header"] and "part_b_latitude" not in parsed_data["header"]:
        parsed_data["header"]["part_b_latitude"] = parsed_data["header"]["part_a_latitude"]
        parsed_data["header"]["part_b_longitude"] = parsed_data["header"]["part_a_longitude"]
    elif "part_b_latitude" in parsed_data["header"] and "part_a_latitude" not in parsed_data["header"]:
        parsed_data["header"]["part_a_latitude"] = parsed_data["header"]["part_b_latitude"]
        parsed_data["header"]["part_a_longitude"] = parsed_data["header"]["part_b_longitude"]
    # If neither is present, ensure they are at least None to avoid errors later.
    if "part_a_latitude" not in parsed_data["header"]:
        parsed_data["header"]["part_a_latitude"] = None
        parsed_data["header"]["part_a_longitude"] = None
    if "part_b_latitude" not in parsed_data["header"]:
        parsed_data["header"]["part_b_latitude"] = None
        parsed_data["header"]["part_b_longitude"] = None


    return parsed_data

def verify_lat_longs(directory: str):
    files = os.listdir(directory)
    for file in files:
        path = os.path.join(directory, file)
        with open(path, 'r') as json_report:
            report = json.load(json_report)
            try:
                assert report['header']['part_a_latitude'] == report['header']['part_b_latitude']
                assert report['header']['part_a_longitude'] == report['header']['part_b_longitude']
            except AssertionError as ae:
                print(f'Verification of {file} coordinates failed: ', ae)
                return False
    return True

def convert_dropsonde_to_stac_item(dropsonde_data: dict, original_filename: str) -> pystac.Item:
    header = dropsonde_data.get('header', {})
    part_a_sounding_system = dropsonde_data.get('part_a_sounding_system', {})
    part_b_significant_wind = dropsonde_data.get('part_b_significant_wind', [])

    try:
        datetime_str_from_filename = original_filename.split('.')[-2]
        id_datetime_part = datetime.strptime(datetime_str_from_filename, '%Y%m%d%H%M').strftime('%Y%m%d%H%M')
        stac_id = f"{header.get('icao_originator', 'unknown')}-{id_datetime_part}-dropsonde"
    except (IndexError, ValueError):
        stac_id = f"dropsonde-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        print(f"Warning: Could not parse datetime from filename '{original_filename}'. Using current UTC time for ID.")

    longitude = header.get('part_a_longitude')
    latitude = header.get('part_a_latitude')

    if longitude is None or latitude is None:
        print("Error: Latitude or Longitude missing from dropsonde header. Cannot create valid geometry.")
        geometry = None
        bbox = None
    else:
        geometry = {
            "type": "Point",
            "coordinates": [longitude, latitude]
        }
        bbox = [longitude, latitude, longitude, latitude]

    try:
        dt_object_utc = datetime.strptime(datetime_str_from_filename, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except (ValueError, NameError):
        dt_object_utc = datetime.now(timezone.utc)
        print("Warning: Could not determine precise datetime from filename. Using current UTC time for STAC datetime.")

    properties = {
        "datetime": dt_object_utc.isoformat(),
        "wmo_header": header.get('wmo_header'),
        "icao_originator": header.get('icao_originator'),
        "transmission_date_time_group": header.get('transmission_date_time_group'),
        "part_a_identifier": header.get('part_a_identifier'),
        "part_a_day": header.get('part_a_day'),
        "part_a_hour": header.get('part_a_hour'),
        "part_a_id_indicator": header.get('part_a_id_indicator'),
        "part_a_latitude": header.get('part_a_latitude'),
        "part_a_quadrant": header.get('part_a_quadrant'),
        "part_a_longitude": header.get('part_a_longitude'),
        "part_a_marsden_square": header.get('part_a_marsden_square'),
        "part_a_ula": header.get('part_a_ula'),
        "part_a_ulo": header.get('part_a_ulo'),
        "part_b_identifier": header.get('part_b_identifier'),
        "part_b_day": header.get('part_b_day'),
        "part_b_hour": header.get('part_b_hour'),
        "part_b_id_indicator": header.get('part_b_id_indicator'),
        "part_b_latitude": header.get('part_b_latitude'),
        "part_b_quadrant": header.get('part_b_quadrant'),
        "part_b_longitude": header.get('part_b_longitude'),
        "part_b_marsden_square": header.get('part_b_marsden_square'),
        "part_b_ula": header.get('part_b_ula'),
        "part_b_ulo": header.get('part_b_ulo'),
        "sounding_system_indicator_raw": part_a_sounding_system.get('sounding_system_indicator_raw'),
        "solar_ir_correction": part_a_sounding_system.get('solar_ir_correction'),
        "radiosonde_system_used": part_a_sounding_system.get('radiosonde_system_used'),
        "tracking_technique_status": part_a_sounding_system.get('tracking_technique_status'),
        "launch_time_indicator": part_a_sounding_system.get('launch_time_indicator'),
        "launch_hour_utc": part_a_sounding_system.get('launch_hour_utc'),
        "launch_minute_utc": part_a_sounding_system.get('launch_minute_utc'),
        "dropsonde:significant_wind": part_b_significant_wind,
        "dropsonde:remarks_mission_info": dropsonde_data.get('remarks', {}).get('mission_info'),
        "dropsonde:remarks_mbl_wnd": dropsonde_data.get('remarks', {}).get('mbl_wnd'),
        "dropsonde:remarks_aev": dropsonde_data.get('remarks', {}).get('aev'),
        "dropsonde:remarks_dlm_wnd": dropsonde_data.get('remarks', {}).get('dlm_wnd'),
        "dropsonde:remarks_wl": dropsonde_data.get('remarks', {}).get('wl')
    }
    item = pystac.Item(
        id=stac_id,
        geometry=geometry,
        bbox=bbox,
        datetime=dt_object_utc,
        properties=properties,
        stac_extensions=[]
    )
    item.add_asset(
        key="raw_dropsonde_message",
        asset=pystac.Asset(
            href=original_filename,
            media_type=pystac.MediaType.JSON,
            title="Raw Dropsonde Message",
            roles=["metadata", "source-data"]
        )
    )
    return item

def main():
    """
    Orchestrates the entire NHC Aircraft Reconnaissance data pipeline:
    1. Downloads raw text files.
    2. Parses raw text files into structured JSON reports.
    3. Verifies parsed report coordinates.
    4. Converts structured reports into STAC Items.
    5. Generates an interactive map from the STAC Items.
    
    Usage: python main.py <URL_TO_REPORTS>
    Example: python main.py https://www.nhc.noaa.gov/archive/recon/2025/REPNT3/
    """
    # 1. Gather Reports
    print("--- Step 1: Gathering Raw Reports ---")
    if len(sys.argv) < 2:
        print("Usage: python main.py <URL_TO_REPORTS>")
        print("Example: python main.py https://www.nhc.noaa.gov/archive/recon/2025/REPNT3/")
        sys.exit(1)
    
    reports_url = sys.argv[1]
    gr.download_reports(reports_url)

    # Ensure 'parsed_reports' and 'stac_items' directories exist (from original main.py)
    os.makedirs('parsed_reports', exist_ok=True)
    os.makedirs('stac_items', exist_ok=True)
    
    # 2. Parse Reports & Generate STAC Items (Original logic from main.py's main)
    print("\n--- Step 2: Parsing Reports and Generating STAC Items ---")
    raw_text_files_dir = 'nhc_text_files'
    rep_list = os.listdir(raw_text_files_dir)
    
    if not rep_list:
        print(f"No raw text files found in '{raw_text_files_dir}'. Exiting parsing and STAC generation.")
        return # Exit if no files to process

    for file_name in rep_list:
        raw_file_path = os.path.join(raw_text_files_dir, file_name)
        with open(raw_file_path, 'r', encoding='utf-8') as temp_drop_file:
            message = temp_drop_file.read()
        
        parsed_result = parse_temp_drop(message)
        
        parsed_output_path = os.path.join('parsed_reports', f'{os.path.splitext(file_name)[0]}.json')
        with open(parsed_output_path, 'w') as result_file:
            json.dump(parsed_result, result_file, indent=4)
        print(f"Parsed and saved: {parsed_output_path}")

    # 3. Verify Coordinates (from original main.py)
    print("\n--- Step 3: Verifying Report Coordinates ---")
    if verify_lat_longs('parsed_reports'):
        print('All report coordinates matched!')
    else:
        print('Coordinate verification failed for some reports.')
        # You might choose to exit here or continue with a warning

    # Convert to STAC Items (from original main.py)
    stac_items_dir = 'stac_items'
    for file_name in rep_list: # Use the original list of raw files to get base names
        parsed_input_path = os.path.join('parsed_reports', f'{os.path.splitext(file_name)[0]}.json')
        raw_original_path_for_stac_asset = os.path.join(raw_text_files_dir, file_name) # Path for the STAC asset href

        try:
            with open(parsed_input_path, 'r') as parsed_report_file:
                report_data = json.load(parsed_report_file)
            
            stac_item = convert_dropsonde_to_stac_item(report_data, raw_original_path_for_stac_asset)
            
            stac_output_path = os.path.join(stac_items_dir, f'{os.path.splitext(file_name)[0]}_stac.json')
            with open(stac_output_path, 'w') as stac_spec_file:
                json.dump(stac_item.to_dict(), stac_spec_file, indent=2)
            print(f"Generated STAC item: {stac_output_path}")
        except FileNotFoundError:
            print(f"Warning: Parsed report {parsed_input_path} not found, skipping STAC item generation for it.")
        except Exception as e:
            print(f"Error generating STAC item for {file_name}: {e}")

    # 4. Generate Map (using map_gen.py)
    print("\n--- Step 4: Generating Interactive Map ---")
    if os.path.exists(stac_items_dir) and os.listdir(stac_items_dir):
        mg.plot_stac_items_from_directory(stac_items_dir, "nhc_dropsondes_map.html")
    else:
        print(f"No STAC items found in '{stac_items_dir}'. Skipping map generation.")
    
    print("\n--- Workflow Complete ---")

if __name__ == '__main__':
    original_argv = sys.argv[:]

    # Check if a URL argument is provided when running this main.py
    if len(original_argv) < 2:
        print("Error: Please provide a URL to download reports from.")
        print("Usage: python main.py <URL_TO_REPORTS_INDEX>")
        print("Example: python main.py https://www.nhc.noaa.gov/archive/recon/2023/REPNT3/")
        sys.exit(1)

    main()

    # Restore sys.argv to its original state if necessary for other parts of the script
    sys.argv = original_argv