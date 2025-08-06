"""
Parses NHC Aircraft Reconnaissance data TEMP DROP text files into STAC items.

.. notes:: TEMP DROP message text files to parse must exist in the nhc_text_files 

.. author:: Zachary Davis <zdavis@csumb.edu>

.. changelog::
    .. versionadded:: 1.0
        Initial release of the module with core functionalities.
"""
import os
import sys
import re
import json
from datetime import datetime, timezone
import pystac
from urllib.parse import urlparse
__version__ = '1.0'

def decode_pressure_height(group: str):
    """Decodes the PnPnhnhnhn group for pressure and height.
    
    This function interprets a 5-digit group as either pressure in hPa
    or geopotential height in meters, based on the first digit.
    
    :param group: The 5-digit string representing pressure/height.
    :type group: str
    :return: A tuple (pressure_mb, height_m). One of them will be None.
    :rtype: tuple
    :raises ValueError: If the group length is not 5 or the first digit is invalid.
    """
    if len(group) != 5:
        raise ValueError("Invalid pressure/height group length, expected 5 digits.")
    
    first_digit = int(group[0])
    
    if 0 <= first_digit <= 5: # PPPP.P hPa (e.g., 10000 -> 1000.0 hPa)
        pressure = float(group) / 10.0
        height = None 
        return pressure, height
    elif 6 <= first_digit <= 8: # hnhnhn decameters (e.g., 60000 -> 6000 meters)
        height = float(group) * 10 # Convert to meters
        pressure = None 
        return pressure, height
    elif first_digit == 9: # 1PPP.P hPa (e.g., 99000 -> 1900.0 hPa, where 90000 is added for pressure > 1000 hPa)
        pressure = (90000 + float(group)) / 10.0
        height = None
        return pressure, height
    else:
        raise ValueError(f"Invalid first digit for pressure/height group: {group}")

def decode_temp_dewpoint(group: str):
    """Decodes the TTTaDD group for temperature and dew-point depression.
    
    :param group: The 5-digit string representing temperature and dew-point depression.
    :type group: str
    :return: A tuple (temperature_c, dew_point_depression_c).
    :rtype: tuple
    :raises ValueError: If the group length is not 5 or the 'Ta' indicator is invalid.
    """
    if len(group) != 5:
        raise ValueError("Invalid temperature/dew-point group length, expected 5 digits.")
    
    TTT = int(group[0:3])
    Ta = int(group[3])
    DD = int(group[4])

    # Temperature decoding: Ta indicates sign (0 for positive/zero, 1 for negative)
    if Ta == 0: 
        temperature = float(TTT) / 10.0
    elif Ta == 1: 
        temperature = -float(TTT) / 10.0
    else:
        raise ValueError(f"Invalid 'Ta' indicator for temperature: {Ta}")

    # Dew-point depression (DD) in tenths of a degree Celsius
    dew_point_depression = float(DD) / 10.0 

    return temperature, dew_point_depression

def decode_wind(group: str):
    """Decodes the dddff group for wind direction and speed.
    
    :param group: The 5-digit string representing wind direction and speed.
    :type group: str
    :return: A tuple (wind_direction_deg, wind_speed_kt). Wind direction can be None.
    :rtype: tuple
    :raises ValueError: If the group length is not 5.
    """
    if len(group) != 5:
        raise ValueError("Invalid wind group length, expected 5 digits.")
    
    ddd = int(group[0:3])
    ff = int(group[3:5])

    # Wind direction (ddd): 000 for calm, 999 for variable/not observed
    if ddd == 0:
        wind_direction = 0 # Calm
    elif ddd == 999:
        wind_direction = None # Variable or not observed
    else:
        wind_direction = ddd

    # Wind speed (ff) in knots
    wind_speed = ff

    return wind_direction, wind_speed

def parse_temp_drop(message: str, uri: str):
    """Parses a TEMP DROP observation message according to the NHOP 2024 Appendix G format.

    This function extracts header information, mandatory and significant levels,
    tropopause, maximum wind data, and provides human-readable parsing for remarks.
    Datetime is gathered from the filename.

    :param message: The raw TEMP DROP message string.
    :type message: str
    :param uri: uri of the message file being parsed
    :type uri: str
    :return: A dictionary containing the parsed data, or None if parsing fails.
    :rtype: dict
    """    
    parsed_uri = urlparse(uri)
    # The 'path' component contains the file path for file:// URIs or the path part of a URL.
    filename = os.path.basename(parsed_uri.path)
    # Extract datetime from filename for message_date and for consistent day parsing
    datetime_str_from_filename = filename.split('.')[-2]
    try:
        id_datetime_part = datetime.strptime(datetime_str_from_filename, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Warning: Could not parse datetime from filename: {filename}. Using current UTC time.")
        id_datetime_part = datetime.now(timezone.utc)

    lines = [line.strip() for line in message.split('\n') if line.strip()]
    parsed_data = {
        "uri": uri,
        "message_date": id_datetime_part,
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
                parsed_data["header"].update({
                    "wmo_header": parts[0],
                    "icao_originator": parts[1],
                    "transmission_date_time_group": parts[2]
                })
            continue

        # Part A Header (XXAA)
        if line.startswith("XXAA"):
            part_a_active = True
            part_b_active = False # Ensure only one part is active at a time
            header_data_parts = line.split()
            if len(header_data_parts) >= 5: # XXAA YYGGId 99LaLaLa QcLoLoLoLo MMMULaULo
                try:
                    # Removed parsing of 'YY' (day) as filename is authoritative
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
                    # Removed parsing of 'YY' (day) as filename is authoritative
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
                    
                    sounding_system_info = {
                        "indicator_raw": srrarasasa,
                        "solar_ir_correction": int(srrarasasa[0]),
                        "radiosonde_system_used": int(srrarasasa[1:3]),
                        "tracking_technique_status": int(srrarasasa[3:5]),
                        "launch_time_indicator": int(launch_time_group[0]),
                        "launch_hour_utc": int(launch_time_group[1:3]),
                        "launch_minute_utc": int(launch_time_group[3:5])
                    }

                    # Add human-readable descriptions for certain fields
                    solar_ir_correction_map = {
                        0: "No correction",
                        1: "Correction applied"
                    }
                    sounding_system_info["solar_ir_correction_description"] = solar_ir_correction_map.get(sounding_system_info["solar_ir_correction"], "Unknown or not applicable")

                    radiosonde_system_map = {
                        96: "Descending radiosonde",
                        # Add other codes as per WMO FM 35-X Ext. TEMP Table 3778
                        # Example: 00-09 for various radiosonde types, 90-99 for special types
                    }
                    sounding_system_info["radiosonde_system_description"] = radiosonde_system_map.get(sounding_system_info["radiosonde_system_used"], "Unknown or not specified")

                    tracking_technique_map = {
                        0: "No tracking",
                        1: "Radar",
                        2: "Radio direction finding",
                        3: "NAVAID (Omega, Loran-C)",
                        4: "GPS",
                        5: "Other satellite navigation",
                        6: "Inertial",
                        7: "Differential GPS",
                        8: "Automatic satellite navigation", # This is the common one for dropsondes
                        # Add other codes as per WMO FM 35-X Ext. TEMP Table 3778
                    }
                    sounding_system_info["tracking_technique_description"] = tracking_technique_map.get(sounding_system_info["tracking_technique_status"], "Unknown or not specified")
                    
                    parsed_data["part_a_sounding_system"] = sounding_system_info
                except ValueError as e:
                    print(f"Warning: Error parsing Section 7 (31313) line: {e}. Skipping sounding system details.")
            continue

        # Section 10: Remarks (61616 and 62626 groups)
        if line.startswith("61616"):
            raw_mission_info = line[6:].strip()
            parsed_mission_info = {}
            
            parts = raw_mission_info.split()
            
            # Heuristic parsing for mission info based on common patterns
            if len(parts) > 0:
                parsed_mission_info["aircraft_identifier"] = parts[0] # e.g., AF305, AF303
            if len(parts) > 1:
                parsed_mission_info["flight_mission_id"] = parts[1] # e.g., 01WSW, 0303A

            # Iterate through remaining parts to identify optional fields
            # This handles flexible order for IOP/Storm Name/OB/Storm Number
            remaining_parts = parts[2:]
            
            # Flags to ensure we don't re-assign certain fields if already found
            found_iop_or_storm_name = False
            found_ob_indicator = False

            for part in remaining_parts:
                if re.match(r'IOP\d+', part) and not found_iop_or_storm_name:
                    parsed_mission_info["intensive_observation_period"] = part
                    found_iop_or_storm_name = True
                elif re.match(r'[A-Z]+', part) and len(part) > 1 and not found_iop_or_storm_name:
                    # Heuristic: Assume all caps, multiple letters is a storm name
                    parsed_mission_info["storm_name"] = part
                    found_iop_or_storm_name = True
                elif part == "OB" and not found_ob_indicator:
                    parsed_mission_info["observation_indicator"] = part
                    found_ob_indicator = True
                elif re.match(r'\d{2}', part) and "observation_indicator" in parsed_mission_info and parsed_mission_info["observation_indicator"] == "OB" and "storm_number" not in parsed_mission_info:
                    # This assumes the number immediately after "OB" is the storm number
                    parsed_mission_info["storm_number"] = part
                else:
                    # Collect any other parts as additional info
                    if "additional_info" not in parsed_mission_info:
                        parsed_mission_info["additional_info"] = []
                    parsed_mission_info["additional_info"].append(part)
            
            if "additional_info" in parsed_mission_info:
                parsed_mission_info["additional_info"] = " ".join(parsed_mission_info["additional_info"]).strip()
                if not parsed_mission_info["additional_info"]: # Remove if empty after join and strip
                    del parsed_mission_info["additional_info"]

            parsed_data["remarks"]["mission_info"] = raw_mission_info # Keep raw for reference
            parsed_data["remarks"]["mission_info_parsed"] = parsed_mission_info
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
                        # Append to existing value if key already exists (for multiple instances of same remark type)
                        if current_key in parsed_data["remarks"]:
                            parsed_data["remarks"][current_key] += " " + segment
                        else:
                            parsed_data["remarks"][current_key] = segment
                    current_key = None # Reset key after assigning value

            # Further parse and make human-readable for specific remark types
            
            # REL (Release Point)
            if "rel" in parsed_data["remarks"] and parsed_data["remarks"]["rel"]:
                rel_content = parsed_data["remarks"]["rel"]
                rel_parsed = {}
                
                # Try to extract time (DD/HHMMZ)
                time_match = re.search(r'(\d{2}/\d{4}Z)', rel_content)
                if time_match:
                    rel_parsed["time_string"] = time_match.group(1)
                    try:
                        day = int(time_match.group(1)[:2])
                        hour = int(time_match.group(1)[3:5])
                        minute = int(time_match.group(1)[5:7])
                        rel_parsed["time_day"] = day
                        rel_parsed["time_hour_utc"] = hour
                        rel_parsed["time_minute_utc"] = minute
                    except ValueError:
                        pass # Keep raw string if parsing fails
                    rel_content = rel_content.replace(time_match.group(1), '').strip()

                # Try to extract location (e.g., 12.3N 45.6W)
                location_match = re.search(r'([\d.]+)([NS])\s+([\d.]+)([EW])', rel_content)
                if location_match:
                    lat_val = float(location_match.group(1))
                    lat_hem = location_match.group(2)
                    lon_val = float(location_match.group(3))
                    lon_hem = location_match.group(4)
                    
                    if lat_hem == 'S': lat_val *= -1
                    if lon_hem == 'W': lon_val *= -1
                    
                    rel_parsed["latitude"] = lat_val
                    rel_parsed["longitude"] = lon_val
                    rel_content = rel_content.replace(location_match.group(0), '').strip()
                
                # Any remaining content is a description
                if rel_content:
                    rel_parsed["description"] = rel_content
                
                parsed_data["remarks"]["rel_parsed"] = rel_parsed
                # Remove original raw 'rel' entry if parsed
                if "rel" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["rel"]
                            
            # SPG (Splash Group) - similar parsing to REL
            if "spg" in parsed_data["remarks"] and parsed_data["remarks"]["spg"]:
                spg_content = parsed_data["remarks"]["spg"]
                spg_parsed = {}

                time_match = re.search(r'(\d{2}/\d{4}Z)', spg_content)
                if time_match:
                    spg_parsed["time_string"] = time_match.group(1)
                    try:
                        day = int(time_match.group(1)[:2])
                        hour = int(time_match.group(1)[3:5])
                        minute = int(time_match.group(1)[5:7])
                        spg_parsed["time_day"] = day
                        spg_parsed["time_hour_utc"] = hour
                        spg_parsed["time_minute_utc"] = minute
                    except ValueError:
                        pass
                    spg_content = spg_content.replace(time_match.group(1), '').strip()

                location_match = re.search(r'([\d.]+)([NS])\s+([\d.]+)([EW])', spg_content)
                if location_match:
                    lat_val = float(location_match.group(1))
                    lat_hem = location_match.group(2)
                    lon_val = float(location_match.group(3))
                    lon_hem = location_match.group(4)
                    
                    if lat_hem == 'S': lat_val *= -1
                    if lon_hem == 'W': lon_val *= -1
                    
                    spg_parsed["latitude"] = lat_val
                    spg_parsed["longitude"] = lon_val
                    spg_content = spg_content.replace(location_match.group(0), '').strip()
                
                if spg_content:
                    spg_parsed["description"] = spg_content
                
                parsed_data["remarks"]["spg_parsed"] = spg_parsed
                # Remove original raw 'spg' entry if parsed
                if "spg" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["spg"]

            # MBL WND (Mean Boundary Layer Wind)
            if "mbl_wnd" in parsed_data["remarks"] and parsed_data["remarks"]["mbl_wnd"]:
                mbl_wnd_str = parsed_data["remarks"]["mbl_wnd"]
                # Expected format: HHMMZ ddd/ff KNOTS AT NNNN FEET
                match = re.search(r'(\d{4}Z)\s+(\d{3})/(\d{2,3})\s+KNOTS AT (\d+)\s+FEET', mbl_wnd_str)
                if match:
                    parsed_data["remarks"]["mbl_wnd_parsed"] = {
                        "time_utc_string": match.group(1),
                        "wind_direction_deg": int(match.group(2)),
                        "wind_speed_kt": int(match.group(3)),
                        "altitude_feet": int(match.group(4))
                    }
                # Remove original raw 'mbl_wnd' entry if parsed
                if "mbl_wnd" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["mbl_wnd"]
            
            # AEV (Aircraft Eye Fix)
            if "aev" in parsed_data["remarks"] and parsed_data["remarks"]["aev"]:
                aev_str = parsed_data["remarks"]["aev"]
                # Expected format: HHMMZ dd.dddN/S ddd.dddE/W PSN
                match = re.search(r'(\d{4}Z)\s+([\d.]+)([NS])\s+([\d.]+)([EW])\s+PSN', aev_str)
                if match:
                    lat_val = float(match.group(2))
                    lat_hemisphere = match.group(3)
                    lon_val = float(match.group(4))
                    lon_hemisphere = match.group(5)
                    
                    if lat_hemisphere == 'S':
                        lat_val *= -1
                    if lon_hemisphere == 'W':
                        lon_val *= -1

                    parsed_data["remarks"]["aev_parsed"] = {
                        "time_utc_string": match.group(1),
                        "latitude": lat_val,
                        "longitude": lon_val
                    }
                # Remove original raw 'aev' entry if parsed
                if "aev" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["aev"]

            # DLM WND (Dropsonde Launch Mission Wind)
            if "dlm_wnd" in parsed_data["remarks"] and parsed_data["remarks"]["dlm_wnd"]:
                dlm_wnd_str = parsed_data["remarks"]["dlm_wnd"]
                # Expected format: ddd/fff at NNNN FT
                match = re.search(r'(\d{3})/(\d+)\s+at\s+(\d+)\s+FT', dlm_wnd_str)
                if match:
                    parsed_data["remarks"]["dlm_wnd_parsed"] = {
                        "wind_direction_deg": int(match.group(1)),
                        "wind_speed_kt": int(match.group(2)),
                        "altitude_feet": int(match.group(3))
                    }
                # Remove original raw 'dlm_wnd' entry if parsed
                if "dlm_wnd" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["dlm_wnd"]

            # WL (Wind Level)
            if "wl" in parsed_data["remarks"] and parsed_data["remarks"]["wl"]:
                wl_str = parsed_data["remarks"]["wl"]
                # Expected format: NNNN FT ddd/fff
                match = re.search(r'(\d+)\s+FT\s+(\d{3})/(\d+)', wl_str)
                if match:
                    parsed_data["remarks"]["wl_parsed"] = {
                        "altitude_feet": int(match.group(1)),
                        "wind_direction_deg": int(match.group(2)),
                        "wind_speed_kt": int(match.group(3))
                    }
                # Remove original raw 'wl' entry if parsed
                if "wl" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["wl"]
            
            # EYEWALL
            if "eyewall" in parsed_data["remarks"] and parsed_data["remarks"]["eyewall"]:
                eyewall_str = parsed_data["remarks"]["eyewall"]
                # Expected format: HHMMZ, NNNN ft
                match = re.search(r'(\d{4}Z),\s+(\d+)\s+ft', eyewall_str)
                if match:
                    parsed_data["remarks"]["eyewall_parsed"] = {
                        "time_utc_string": match.group(1),
                        "altitude_feet": int(match.group(2))
                    }
                # Remove original raw 'eyewall' entry if parsed
                if "eyewall" in parsed_data["remarks"]:
                    del parsed_data["remarks"]["eyewall"]
            continue

        # Data lines for Part A (Mandatory Levels, Tropopause, Max Wind)
        if part_a_active:
            # Mandatory Levels (Section 2)
            # Lines containing repeating groups of PnPnhnhnhn TTTaDD dddff
            # This regex looks for 5-digit numbers separated by spaces, assuming groups of 3 for each level
            # It handles both PPPP.P and HHH.H (decameters) for the first group.
            if re.match(r'^(\d{5}\s+){2}\d{5}(\s+\d{5}\s+\d{5}\s+\d{5})*$', line):
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


def convert_dropsonde_to_stac_item(dropsonde_data: dict) -> pystac.Item:
    """Converts a parsed dropsonde message (dictionary) into a pystac.Item,
    focused on "who, what, when, and where" metadata.

    :param dropsonde_data: The parsed dropsonde message as a dictionary
    :type dropsonde_data: dict
    :return: A pystac Item representing the dropsonde data
    :rtype: pystac.Item
    """    
    header = dropsonde_data.get('header', {})
    part_a_sounding_system = dropsonde_data.get('part_a_sounding_system')
    if part_a_sounding_system is None:
        part_a_sounding_system = {}  # Ensure it's a dict to avoid KeyError
    remarks = dropsonde_data.get('remarks', {})
    dt_utc_string = dropsonde_data['message_date'].isoformat().replace('+00:00', 'Z').replace(':', '-') # Ensure 'Z' for UTC and replace : in time

    # 1. STAC ID: Create a unique ID for the STAC Item.
    stac_id = f"{header.get('wmo_header', 'unknown')}-{header.get('icao_originator', 'unknown')}-{dt_utc_string}-dropsonde"

    # 2. Geometry and Bounding Box: Represent the dropsonde's launch location.
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
        # For a point, the bbox is [lon, lat, lon, lat]
        bbox = [longitude, latitude, longitude, latitude]

    # 3. Properties: Add only "who, what, when, and where" metadata.
    properties = {
        # STAC Common Metadata fields
        "datetime": dt_utc_string, # ISO 8601 format with Z for UTC

        # Who (Originator)
        "dropsonde:icao_originator": header.get('icao_originator'),

        # What (Type of observation, mission, system used)
        "dropsonde:wmo_header": header.get('wmo_header'),
        "dropsonde:radiosonde_system_description": part_a_sounding_system.get('radiosonde_system_description'),
        "dropsonde:tracking_technique_description": part_a_sounding_system.get('tracking_technique_description'),

        # When (Observation time, transmission time, launch time)
        "dropsonde:transmission_date_time_group": header.get('transmission_date_time_group'),
        "dropsonde:launch_hour_utc": part_a_sounding_system.get('launch_hour_utc'),
        "dropsonde:launch_minute_utc": part_a_sounding_system.get('launch_minute_utc'),

        # Where (Location)
        "dropsonde:latitude": header.get('part_a_latitude'),
        "dropsonde:longitude": header.get('part_a_longitude'),
        "dropsonde:marsden_square": header.get('part_a_marsden_square'),
    }

    # Add parsed mission info if available
    if 'mission_info_parsed' in remarks:
        for key, value in remarks['mission_info_parsed'].items():
            properties[f"dropsonde:mission_info_{key}"] = value
    # If there was a raw mission_info but no structured parsing, keep the raw version
    elif 'mission_info' in remarks: 
        properties["dropsonde:mission_info_raw"] = remarks['mission_info']


    # Add parsed remarks if they exist
    if 'mbl_wnd_parsed' in remarks:
        properties["dropsonde:remarks_mbl_wnd_parsed"] = remarks['mbl_wnd_parsed']
    if 'aev_parsed' in remarks:
        properties["dropsonde:remarks_aev_parsed"] = remarks['aev_parsed']
    if 'dlm_wnd_parsed' in remarks:
        properties["dropsonde:remarks_dlm_wnd_parsed"] = remarks['dlm_wnd_parsed']
    if 'wl_parsed' in remarks:
        properties["dropsonde:remarks_wl_parsed"] = remarks['wl_parsed']
    if 'rel_parsed' in remarks:
        properties["dropsonde:remarks_rel_parsed"] = remarks['rel_parsed']
    if 'spg_parsed' in remarks:
        properties["dropsonde:remarks_spg_parsed"] = remarks['spg_parsed']
    if 'eyewall_parsed' in remarks:
        properties["dropsonde:remarks_eyewall_parsed"] = remarks['eyewall_parsed']
    # Add initial description if it exists and is not empty
    if 'initial_description' in remarks and remarks['initial_description'].strip():
        properties["dropsonde:remarks_initial_description"] = remarks['initial_description'].strip()


    # Create the pystac Item
    item = pystac.Item(
        id=stac_id,
        geometry=geometry,
        bbox=bbox,
        datetime=dropsonde_data['message_date'],
        properties=properties,
        stac_extensions=[] # Add relevant extensions here if you use them (e.g., "https://stac-extensions.github.io/forecast/v0.2.0/schema.json")
    )

    # Add the original dropsonde TEXT file as an asset to the STAC Item
    item.add_asset(
        key="raw_dropsonde_message",
        asset=pystac.Asset(
            href=dropsonde_data['uri'],
            media_type=pystac.MediaType.TEXT,
            title="Raw Dropsonde Message",
            roles=["metadata", "source-data"] # Define roles for the asset
        )
    )

    return item
