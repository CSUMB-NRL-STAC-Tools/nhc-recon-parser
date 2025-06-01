"""
Plots report locations using STAC items

.. author:: Zachary Davis <zdavis@csumb.edu>

.. changelog::
    .. versionadded:: 1.0
        Initial release of the module with core functionalities.
"""
import os
import json
import folium
__version__ = '1.0'

# Extract coordinates (longitude, latitude) from the STAC Item's geometry
# Note: Folium expects (latitude, longitude) for map centering and marker placement
with open('stac_items/REPPA3-KNHC.202502030505_stac.json', 'r') as temp:
    stac_item_example = json.load(temp)

def plot_single_stac_item(stac_item: dict, output_file: str = "single_dropsonde_stac_map.html"):
    """Plots a single STAC Item on a Folium map and saves it to an HTML file"""
    longitude = stac_item['geometry']['coordinates'][0]
    latitude = stac_item['geometry']['coordinates'][1]

    m = folium.Map(location=[latitude, longitude], zoom_start=8, tiles='OpenStreetMap')

    popup_html = f"""
        <b>STAC Item ID:</b> {stac_item['id']}<br>
        <b>Observation Time (UTC):</b> {stac_item['properties'].get('datetime', 'N/A')}<br>
        <b>Originator:</b> {stac_item['properties'].get('icao_originator', 'N/A')}<br>
        <b>Significant Wind Levels:</b> {stac_item['properties'].get('dropsonde:significant_wind_levels', 'N/A')}<br>
        <b>Asset Filename:</b> {stac_item['assets'].get('raw_dropsonde_message', 'N/A')}<br>
        <br>
        <i>Full STAC Item details available as asset.</i>
    """

    folium.Marker(
        location=[latitude, longitude],
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=stac_item['id']
    ).add_to(m)

    m.save(output_file)
    print(f"Interactive map for single item saved to {output_file}")
    print(f"Open '{output_file}' in your web browser to view the map.")

def plot_stac_items_from_directory(directory_path: str, output_file: str = "multiple_dropsondes_stac_map.html"):
    """Reads STAC item JSON files from a specified directory and plots each item
    
    Each STAC item is represented as a marker on a single Folium map, and is
    saved to an HTML output file.
    
    :param directory_path: The path to the directory containing STAC Item JSON files
    :type directory_path: str
    :param output_file: The name of the HTML file to save the map, defaults to "multiple_dropsondes_stac_map.html"
    :type output_file: str, optional
    """    
    stac_items = []
    # Iterate through files in the given directory
    for filename in os.listdir(directory_path):
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)
            try:
                with open(file_path, 'r') as f:
                    item_data = json.load(f)
                    # Basic validation to ensure it looks like a STAC Item
                    if "type" in item_data and item_data["type"] == "Feature" and \
                       "geometry" in item_data and "properties" in item_data and \
                       "coordinates" in item_data["geometry"]:
                        stac_items.append(item_data)
                    else:
                        print(f"Skipping '{filename}': Not a valid STAC Item structure.")
            except json.JSONDecodeError:
                print(f"Skipping '{filename}': Invalid JSON format.")
            except Exception as e:
                print(f"Error reading '{filename}': {e}")

    if not stac_items:
        print(f"No valid STAC Items found in '{directory_path}'. No map will be generated.")
        return

    # Initialize map: Use the first item's location to center the map initially
    first_item_coords = stac_items[0]['geometry']['coordinates']
    m = folium.Map(location=[first_item_coords[1], first_item_coords[0]], zoom_start=6, tiles='OpenStreetMap')

    # Add a marker for each STAC Item
    for item in stac_items:
        try:
            longitude = item['geometry']['coordinates'][0]
            latitude = item['geometry']['coordinates'][1]
            item_id = item.get('id', 'N/A')
            obs_datetime = item['properties'].get('datetime', 'N/A')
            originator = item['properties'].get('icao_originator', 'N/A')
            wind_levels = item['properties'].get('dropsonde:significant_wind_levels', 'N/A')
            asset_filename = item['assets']['raw_dropsonde_message'].get('href', 'N/A')

            popup_html = f"""
                <b>STAC Item ID:</b> {item_id}<br>
                <b>Observation Time (UTC):</b> {obs_datetime}<br>
                <b>Originator:</b> {originator}<br>
                <b>Significant Wind Levels:</b> {wind_levels}<br>
                <b>Asset Filename:</b> {asset_filename}<br>
                <br>
                <i>Full STAC Item details available as asset.</i>
            """
            folium.Marker(
                location=[latitude, longitude],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=item_id
            ).add_to(m)
        except KeyError as e:
            print(f"Skipping item due to missing key for plotting: {e} in item ID {item.get('id', 'Unknown')}")
        except Exception as e:
            print(f"An error occurred while processing an item: {e} in item ID {item.get('id', 'Unknown')}")

    # Save the combined map
    m.save(output_file)
    print(f"\nInteractive map with multiple items saved to {output_file}")
    print(f"Open '{output_file}' in your web browser to view the map.")

def main():
    """Plots all STAC items from the stac_items directory onto a map"""    
    plot_stac_items_from_directory('stac_items', "multiple_dropsondes_stac_map.html")

if __name__ == '__main__':
    main()