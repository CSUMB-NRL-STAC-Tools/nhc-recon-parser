"""
Parses NHC Aircraft Reconnaissance data TEMP DROP text files into STAC items.

.. seealso:: :py:mod:`gather_reports`, :py:mod:`map_gen`
.. notes:: TEMP DROP message text files to parse must exist in the nhc_text_files 

.. author:: Zachary Davis <zdavis@csumb.edu>

.. changelog::
    .. versionadded:: 1.0
        Initial release of the module with core functionalities.
"""
import os
import re
import json
from datetime import datetime, timezone
import pystac
__version__ = '1.0'

def decode_wind(wind_group_str: str):
    """ Decodes a 5-digit wind group (dndnfnfnfn) into true wind direction and speed

    Format: dndn (hundreds/tens of degrees) + f_hundreds_plus_d_unit (hundreds of speed + unit of direction) + f_tens_units (tens/units of speed).
    Example: 29625 -> 295 degrees, 125 knots.
    Example: 09596 -> 095 degrees, 096 knots.

    :param wind_group_str: 5-digit wind group
    :type wind_group_str: str
    :return: wind direction and speed
    :rtype: tuple
    """    
    if len(wind_group_str) != 5 or not wind_group_str.isdigit():
        return None, None # Invalid format

    d_hundreds_tens = int(wind_group_str[0:2])
    f_hundreds_plus_d_unit = int(wind_group_str[2])
    f_tens_units = int(wind_group_str[3:5])

    # Deduce the unit digit of direction (0 or 5) and the actual hundreds digit of speed
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
    """Decodes a 5-digit temperature/dew point depression group (TTTDD)

    Assumes TTT is temperature in tenths of degrees Celsius, and DD is dew point depression in whole degrees Celsius.
    Example: 22606 -> Temperature: 22.6 C, Dew Point Depression: 6 C.
    
    :param temp_dew_group_str: 5-digit temperature/dew point depression group
    :type temp_dew_group_str: str
    :return: temperature and dew point depression
    :rtype: tuple
    """    
    if len(temp_dew_group_str) != 5 or not temp_dew_group_str.isdigit():
        return None, None

    # Interpret TTT as temperature in tenths, DD as dew point depression
    temperature = float(temp_dew_group_str[0:3]) / 10.0
    dew_point_depression = int(temp_dew_group_str[3:5])

    return temperature, dew_point_depression

def decode_pressure_height(pressure_height_group_str: str):
    """Decodes a 5-digit pressure/height group (PPP_HH)

    Assumes PPP is pressure in whole millibars, and HH is height in tens of meters.
    Example: 92510 -> Pressure: 925 mb, Height: 100 m.

    :param pressure_height_group_str: 5-digit pressure/height group (PPP_HH)
    :type pressure_height_group_str: str
    :return: pressure and height
    :rtype: tuple
    """    
    if len(pressure_height_group_str) != 5 or not pressure_height_group_str.isdigit():
        return None, None

    pressure = int(pressure_height_group_str[0:3])
    height = int(pressure_height_group_str[3:5]) * 10 # Convert tens of meters to meters

    return pressure, height

def parse_temp_drop(message: str):
    """Parses a TEMP DROP observation message according to the NHOP 2024 Appendix G format.

    :param message: The raw TEMP DROP message string
    :type message: str
    :return: A dictionary containing the parsed data, or None if parsing fails
    :rtype: dict
    """    
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

    part_a_active = False
    part_b_active = False

    for i, line in enumerate(lines):
        # Skip empty lines
        if not line:
            continue
        
        # WMO Header Line (always first line of the message)
        if i == 0:
            parsed_data["header"] = {
                "sonde_serial": line,
            }
            continue

        # WMO Header Line (always second line of the message)
        if i == 1:
            parts = line.split()
            if len(parts) >= 3:
                parsed_data["header"] = {
                    "wmo_header": parts[0],
                    "icao_originator": parts[1],
                    "transmission_date_time_group": parts[2]
                }
            continue

        # Part A Header (XXAA)
        if line.startswith("XXAA"):
            part_a_active = True
            part_b_active = False # Ensure only one part is active at a time
            header_data_parts = line.split()
            if len(header_data_parts) >= 5: # XXAA YYGGId 99LaLaLa QcLoLoLoLo MMMULaULo
                try:
                    parsed_data["header"]["part_a_identifier"] = header_data_parts[0] # XXAA
                    parsed_data["header"]["part_a_day"] = int(header_data_parts[1][0:2])
                    parsed_data["header"]["part_a_hour"] = int(header_data_parts[1][2:4])
                    parsed_data["header"]["part_a_id_indicator"] = int(header_data_parts[1][4])
                    
                    # Latitude: 99LaLaLa (99 is indicator, LaLaLa is degrees and tenths)
                    lat_str = header_data_parts[2]
                    parsed_data["header"]["part_a_latitude"] = float(lat_str[2:]) / 10.0
                    
                    # Longitude: QcLoLoLoLo (Qc is quadrant, LoLoLoLo is degrees and tenths)
                    lon_str = header_data_parts[3]
                    quadrant_a = int(lon_str[0])
                    parsed_data["header"]["part_a_quadrant"] = quadrant_a
                    parsed_data["header"]["part_a_longitude"] = float(lon_str[1:]) / 10.0

                    # Apply quadrant correction for Part A
                    if quadrant_a in [3, 5]: # South (negative latitude)
                        parsed_data["header"]["part_a_latitude"] *= -1
                    if quadrant_a in [5, 7]: # West (negative longitude)
                        parsed_data["header"]["part_a_longitude"] *= -1

                    # Marsden Square: MMMULaULo
                    marsden_str = header_data_parts[4]
                    parsed_data["header"]["part_a_marsden_square"] = int(marsden_str[0:3])
                    parsed_data["header"]["part_a_ula"] = int(marsden_str[3]) # Ula (Quadrant)
                    parsed_data["header"]["part_a_ulo"] = int(marsden_str[4]) # Ulo (Longitude tens of degrees)

                except ValueError as e:
                    print(f"Warning: Error parsing XXAA header line: {e}. Skipping header details.")
            continue

        # Part B Header (XXBB)
        if line.startswith("XXBB"):
            part_b_active = True
            part_a_active = False # Ensure only one part is active at a time
            header_data_parts = line.split()
            if len(header_data_parts) >= 5: # XXBB YYGG8 99LaLaLa QcLoLoLoLo MMMULaULo
                try:
                    parsed_data["header"]["part_b_identifier"] = header_data_parts[0] # XXBB
                    parsed_data["header"]["part_b_day"] = int(header_data_parts[1][0:2])
                    parsed_data["header"]["part_b_hour"] = int(header_data_parts[1][2:4])
                    parsed_data["header"]["part_b_id_indicator"] = int(header_data_parts[1][4]) # Should be 8
                    
                    lat_str = header_data_parts[2]
                    parsed_data["header"]["part_b_latitude"] = float(lat_str[2:]) / 10.0
                    
                    lon_str = header_data_parts[3]
                    quadrant_b = int(lon_str[0])
                    parsed_data["header"]["part_b_quadrant"] = quadrant_b
                    parsed_data["header"]["part_b_longitude"] = float(lon_str[1:]) / 10.0

                    # Apply quadrant correction for Part B
                    if quadrant_b in [3, 5]: # South (negative latitude)
                        parsed_data["header"]["part_b_latitude"] *= -1
                    if quadrant_b in [5, 7]: # West (negative longitude)
                        parsed_data["header"]["part_b_longitude"] *= -1

                    marsden_str = header_data_parts[4]
                    parsed_data["header"]["part_b_marsden_square"] = int(marsden_str[0:3])
                    parsed_data["header"]["part_b_ula"] = int(marsden_str[3])
                    parsed_data["header"]["part_b_ulo"] = int(marsden_str[4])

                except ValueError as e:
                    print(f"Warning: Error parsing XXBB header line: {e}. Skipping header details.")
            continue

        # Section 7: Sounding System, Radiosonde/System Status, Launch Time (31313 group)
        if line.startswith("31313"):
            parts = line.split()
            if len(parts) >= 3:
                try:
                    srrarasasa = parts[1]
                    launch_time_group = parts[2]
                    parsed_data["part_a_sounding_system"] = { # This section applies to both Part A and B
                        "sounding_system_indicator_raw": srrarasasa,
                        "solar_ir_correction": int(srrarasasa[0]),
                        "radiosonde_system_used": int(srrarasasa[1:3]), # 96 for Descending radiosonde
                        "tracking_technique_status": int(srrarasasa[3:5]), # 08 for Automatic satellite navigation
                        "launch_time_indicator": int(launch_time_group[0]), # Should be 8
                        "launch_hour_utc": int(launch_time_group[1:3]),
                        "launch_minute_utc": int(launch_time_group[3:5])
                    }
                except ValueError as e:
                    print(f"Warning: Error parsing Section 7 (31313) line: {e}. Skipping sounding system details.")
            continue

        # Section 10: Remarks (61616 and 62626 groups)
        if line.startswith("61616"):
            parsed_data["remarks"]["mission_info"] = line[6:].strip()
            continue
        if line.startswith("62626"):
            remark_string = line[6:].strip()
            # Use regex to split by known remark keys, keeping the keys
            remark_segments = re.split(r'(MBL WND|AEV|DLM WND|WL|REL|SPG|EYEWALL)', remark_string)
            
            current_key = "initial_description" # Default key for the first segment
            for segment in remark_segments:
                segment = segment.strip()
                if not segment:
                    continue
                
                # Check if the segment is one of the known keys
                if segment in ["MBL WND", "AEV", "DLM WND", "WL", "REL", "SPG", "EYEWALL"]:
                    current_key = segment.replace(" ", "_").lower() # Convert to snake_case for dictionary key
                else:
                    if current_key: # Only assign if a key is active
                        parsed_data["remarks"][current_key] = segment
                    current_key = None # Reset key after assigning value

            # Further parse REL and SPG for location and time if available
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

        # Data lines for Part A (Mandatory Levels, Tropopause, Max Wind)
        if part_a_active:
            # Mandatory Levels (Section 2)
            # Lines containing repeating groups of PnPnhnhnhn TTTaDD dddff
            # This regex looks for 5-digit numbers separated by spaces, assuming groups of 3 for each level
            if re.match(r'^(\d{5}\s+){2}\d{5}(\s+\d{5}\s+\d{5}\s+\d{5})*$', line) or \
               re.match(r'^\d{3}\d{2}\s+\d{3}\d{2}\s+\d{5}(\s+\d{3}\d{2}\s+\d{3}\d{2}\s+\d{5})*$', line): # More specific for PPPP HH TTTDD DDDFF
                groups = line.split()
                # Process groups in sets of 3 (pressure/height, temp/dewpoint, wind)
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
                continue # Move to next line

            # Tropopause Level (Section 3: 88PtPtPt TtTtTatDtDt dtdtftftft or 88999)
            if line.startswith("88"):
                parts = line.split()
                if parts[0] == "88999":
                    parsed_data["part_a_tropopause"] = {"not_observed": True}
                elif len(parts) >= 3:
                    try:
                        pressure = int(parts[0][2:]) # 88PtPtPt
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

            # Maximum Wind Data (Section 4: 77PmPmPm dmdmfmfmfm (4vbvbvava) or 66PmPmPm ...)
            if line.startswith("77") or line.startswith("66"):
                parts = line.split()
                if parts[0] == "77999":
                    parsed_data["part_a_max_wind"] = {"not_observed": True}
                elif len(parts) >= 2:
                    try:
                        pressure = int(parts[0][2:]) # 77PmPmPm or 66PmPmPm
                        wind_dir, wind_speed = decode_wind(parts[1])
                        max_wind_data = {
                            "indicator": parts[0][0:2], # 77 or 66
                            "pressure_mb": pressure,
                            "wind_direction_deg": wind_dir,
                            "wind_speed_kt": wind_speed
                        }
                        if len(parts) > 2 and parts[2].startswith("4"): # Vertical wind shear (4vbvbvava)
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

        # Data lines for Part B (Significant Levels)
        if part_b_active:
            # Significant Temperature and Humidity Levels (Section 5: nonoPoPoPo ToToTaoDoDo)
            # This regex looks for repeating groups of 5-digit numbers for level info and temp/dewpoint
            if re.match(r'^(\d{5}\s+){1}\d{5}(\s+\d{5}\s+\d{5})*$', line):
                groups = line.split()
                # Process groups in sets of 2 (level/pressure, temp/dewpoint)
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

            # Significant Wind Levels (Section 6: 21212 nonoPoPoPo dodofofofo)
            if line.startswith("21212"):
                data_string = line[6:].strip() # Remove "21212 "
                groups = data_string.split()
                # Process groups in sets of 2 (level/pressure, wind)
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

    return parsed_data

def verify_lat_longs(directory: str):
    """Checks if parsed Latitude and Longitudes from part A and B match

    :param directory: directory of parsed TEMP DROP json files
    :type directory: str
    :return: all latitude and longitudes matched
    :rtype: bool
    """    
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
    """Converts a parsed dropsonde message (dictionary) into a pystac.Item

    :param dropsonde_data: The parsed dropsonde message as a dictionary
    :type dropsonde_data: dict
    :param original_filename: The filename of the original dropsonde JSON data.
    :type original_filename: str
    :return: A pystac Item representing the dropsonde data
    :rtype: pystac.Item
    """    
    header = dropsonde_data.get('header', {})
    part_a_sounding_system = dropsonde_data.get('part_a_sounding_system', {})
    part_b_significant_wind = dropsonde_data.get('part_b_significant_wind', [])

    # 1. STAC ID: Create a unique ID for the STAC Item.
    # We'll use the ICAO originator and the transmission date/time from the filename
    # for a unique identifier. Assuming the filename format is consistent.
    # Example filename: REPPA3-KNHC.202502030504.json
    try:
        # Extract date and time from the filename (e.g., '202502030504')
        datetime_str_from_filename = original_filename.split('.')[-2]
        # Format it into a more readable string for the ID
        id_datetime_part = datetime.strptime(datetime_str_from_filename, '%Y%m%d%H%M').strftime('%Y%m%d%H%M')
        stac_id = f"{header.get('icao_originator', 'unknown')}-{id_datetime_part}-dropsonde"
    except (IndexError, ValueError):
        # Fallback if filename parsing fails
        stac_id = f"dropsonde-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        print(f"Warning: Could not parse datetime from filename '{original_filename}'. Using current UTC time for ID.")


    # 2. Geometry and Bounding Box: Represent the dropsonde's launch location.
    # The dropsonde data contains latitude and longitude in the header.
    # Use the corrected latitude/longitude from the parsed data
    longitude = header.get('part_a_longitude')
    latitude = header.get('part_a_latitude')

    if longitude is None or latitude is None:
        print("Error: Latitude or Longitude missing from dropsonde header. Cannot create valid geometry.")
        # Provide a default or raise an error, depending on desired behavior
        geometry = None
        bbox = None
    else:
        geometry = {
            "type": "Point",
            "coordinates": [longitude, latitude]
        }
        # For a point, the bbox is [lon, lat, lon, lat]
        bbox = [longitude, latitude, longitude, latitude]

    # 3. Datetime: The primary temporal extent of the asset.
    # We'll use the transmission date/time from the filename, converted to UTC.
    try:
        # Assuming filename datetime is UTC
        dt_object_utc = datetime.strptime(datetime_str_from_filename, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except (ValueError, NameError):
        # Fallback if filename datetime parsing failed or datetime_str_from_filename is not defined
        dt_object_utc = datetime.now(timezone.utc)
        print("Warning: Could not determine precise datetime from filename. Using current UTC time for STAC datetime.")

    # 4. Properties: Add all relevant metadata from the dropsonde message.
    # We'll include various header fields, sounding system details, and significant wind data.
    properties = {
        # STAC Common Metadata fields
        "datetime": dt_object_utc.isoformat(), # ISO 8601 format with Z for UTC

        # Dropsonde-specific metadata from the header
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

        # Sounding system information
        "sounding_system_indicator_raw": part_a_sounding_system.get('sounding_system_indicator_raw'),
        "solar_ir_correction": part_a_sounding_system.get('solar_ir_correction'),
        "radiosonde_system_used": part_a_sounding_system.get('radiosonde_system_used'),
        "tracking_technique_status": part_a_sounding_system.get('tracking_technique_status'),
        "launch_time_indicator": part_a_sounding_system.get('launch_time_indicator'),
        "launch_hour_utc": part_a_sounding_system.get('launch_hour_utc'),
        "launch_minute_utc": part_a_sounding_system.get('launch_minute_utc'),

        # Significant wind data (custom property with a namespace prefix 'dropsonde:')
        "dropsonde:significant_wind": part_b_significant_wind,

        # Remarks (if present)
        "dropsonde:remarks_mission_info": dropsonde_data.get('remarks', {}).get('mission_info'),
        "dropsonde:remarks_mbl_wnd": dropsonde_data.get('remarks', {}).get('mbl_wnd'),
        "dropsonde:remarks_aev": dropsonde_data.get('remarks', {}).get('aev'),
        "dropsonde:remarks_dlm_wnd": dropsonde_data.get('remarks', {}).get('dlm_wnd'),
        "dropsonde:remarks_wl": dropsonde_data.get('remarks', {}).get('wl')
    }

    # Create the pystac Item
    item = pystac.Item(
        id=stac_id,
        geometry=geometry,
        bbox=bbox,
        datetime=dt_object_utc,
        properties=properties,
        stac_extensions=[] # Add relevant extensions here if you use them (e.g., "https://stac-extensions.github.io/forecast/v0.2.0/schema.json")
    )

    # Add the original dropsonde JSON file as an asset to the STAC Item
    item.add_asset(
        key="raw_dropsonde_message",
        asset=pystac.Asset(
            href=original_filename,
            media_type=pystac.MediaType.JSON,
            title="Raw Dropsonde Message",
            roles=["metadata", "source-data"] # Define roles for the asset
        )
    )

    return item

def main():
    """Parses TEMP DROP messages, verifys coordinates, and generates STAC items"""    
    # Ensure 'parsed_reports' and 'stac_items' directories exist
    os.makedirs('parsed_reports', exist_ok=True)
    os.makedirs('stac_items', exist_ok=True)
    
    rep_list = os.listdir('nhc_text_files')
    for file in rep_list:
        with open(os.path.join('nhc_text_files', file), 'r', encoding='utf-8') as temp_drop_file:
            message = temp_drop_file.read()
        res = parse_temp_drop(message)
        with open(os.path.join('parsed_reports', f'{os.path.splitext(file)[0]}.json'), 'w') as result:
            json.dump(res, result, indent=4) 
    
    if verify_lat_longs('parsed_reports'):
        print('All report coordinates matched!')

    for file in rep_list:
        with open(os.path.join('parsed_reports', f'{os.path.splitext(file)[0]}.json'), 'r') as temp_drop_file:
            report = json.load(temp_drop_file)
            stac_item = convert_dropsonde_to_stac_item(report, os.path.join('nhc_text_files', file))
            with open(os.path.join('stac_items', f'{os.path.splitext(file)[0]}_stac.json'), 'w') as stac_spec_file:
                json.dump(stac_item.to_dict(), stac_spec_file, indent=2)

if __name__ == '__main__':
    main()
