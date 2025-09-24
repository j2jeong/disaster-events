#!/usr/bin/env python3
"""
Fix RSOE events with missing coordinates using address-based mapping
"""
import json
import re
from datetime import datetime

def get_coordinates_from_address(address):
    """Get approximate coordinates from address string"""
    if not address:
        return "", ""

    address_lower = address.lower().strip()

    # City/location coordinates (approximate)
    location_coords = {
        # Countries
        'france': ('46.23', '2.21'),
        'nepal': ('28.39', '84.12'),
        'ukraine': ('48.38', '31.17'),

        # Cities and regions
        'khartoum': ('15.55', '32.53'),
        'sudan': ('15.55', '32.53'),  # Khartoum coordinates for Sudan capital
        'port-au-prince': ('18.54', '-72.34'),
        'haiti': ('18.54', '-72.34'),
        'utah': ('39.32', '-111.09'),
        'orem': ('40.30', '-111.69'),
        'madrid': ('40.42', '-3.70'),
        'spain': ('40.42', '-3.70'),  # Madrid coordinates for Spain
        'gaza': ('31.35', '34.31'),
        'israel': ('31.35', '34.31'),  # Gaza coordinates
        'red sea': ('20.00', '38.00'),
        'bab el-mandeb': ('12.58', '43.42'),
        'yemen': ('12.58', '43.42'),  # Bab el-Mandeb area

        # Additional locations
        'bulgaria': ('42.73', '25.49'),
        'pakistan': ('30.38', '69.35'),
        'kunar': ('34.84', '71.09'),  # Kunar province coordinates
        'nangarhar': ('34.17', '70.61'),  # Nangarhar province coordinates
        'afghanistan': ('34.84', '71.09'),  # Use Kunar coords for Afghanistan
        'darfur': ('14.03', '25.00'),  # Central Darfur
        'sudan': ('14.03', '25.00'),  # Use Darfur coords for Sudan
        'pleven': ('43.42', '24.61'),  # Pleven Oblast, Bulgaria
    }

    # Try to match location names
    for location, coords in location_coords.items():
        if location in address_lower:
            return coords

    return "", ""

def process_file(path: str) -> int:
    updated_count = 0
    try:
        with open(path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load {path}: {e}")
        return 0

    for event in data:
        is_rsoe = not event.get('data_source') or event.get('data_source') == 'rsoe'
        if not is_rsoe:
            continue

        current_lat = str(event.get('latitude', '')).strip()
        current_lon = str(event.get('longitude', '')).strip()

        # Treat strings like "0" or 0.0 as missing
        def is_missing(v: str) -> bool:
            try:
                if v in ('', None):
                    return True
                f = float(v)
                return f == 0.0
            except Exception:
                return True

        if is_missing(current_lat) or is_missing(current_lon):
            address = event.get('address', '')
            if address:
                lat, lon = get_coordinates_from_address(address)
                if lat and lon:
                    event['latitude'] = lat
                    event['longitude'] = lon
                    updated_count += 1
                    print(f"✅ Updated {event.get('event_id')}: {address} -> {lat}, {lon} ({path})")
                else:
                    print(f"⚠️ No coordinates found for: {address} ({path})")

    if updated_count:
        # backup
        backup_file = f"{path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            with open(backup_file, 'w') as f:
                json.dump(data, f, indent=2)
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"💾 Saved {updated_count} updates to {path} (backup: {backup_file})")
        except Exception as e:
            print(f"⚠️ Failed to save {path}: {e}")
            return 0
    return updated_count


def main():
    print("🔄 Fixing RSOE events with missing coordinates (events + past_events)...")
    total = 0
    total += process_file('docs/data/events.json')
    total += process_file('docs/data/past_events.json')
    if total == 0:
        print("ℹ️ No RSOE events needed coordinate updates")

if __name__ == "__main__":
    main()
