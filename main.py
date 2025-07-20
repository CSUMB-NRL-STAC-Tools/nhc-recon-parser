from nhc_recon_parser import gather_reports, parser, api_util
import json

url = 'https://www.nhc.noaa.gov/archive/recon/2024/REPNT3/REPNT3-KNHC.202401232347.txt'
dropsonde_report = parser.parse_temp_drop(*gather_reports.read_dropsonde_message(url))
stac_item = parser.convert_dropsonde_to_stac_item(dropsonde_report, url)
print(stac_item.to_dict())

try:
    print(api_util.add_item_to_collection(stac_item, 'dropsonde', 'http://54.193.30.87'))
except Exception as e:
    print(e)

with open('temp.json', 'w') as test_item:
    json.dump(stac_item.to_dict(), test_item, indent=4)