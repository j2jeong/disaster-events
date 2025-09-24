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

def main():
    print("🔄 Fixing RSOE events with missing coordinates...")

    # Load current events
    events_file = 'docs/data/events.json'
    with open(events_file, 'r') as f:
        events = json.load(f)

    print(f"📊 Total events loaded: {len(events)}")

    updated_count = 0

    for event in events:
        # Check if it's an RSOE event (no data_source field or data_source == 'rsoe')
        is_rsoe = not event.get('data_source') or event.get('data_source') == 'rsoe'

        if is_rsoe:
            current_lat = event.get('latitude', '')
            current_lon = event.get('longitude', '')

            # Check if coordinates are missing or empty
            needs_update = (current_lat == '' or current_lon == '' or
                          current_lat == '0' or current_lon == '0')

            if needs_update:
                address = event.get('address', '')
                if address:
                    lat, lon = get_coordinates_from_address(address)
                    if lat and lon:
                        event['latitude'] = lat
                        event['longitude'] = lon
                        updated_count += 1
                        print(f"✅ Updated {event['event_id']}: {address} -> {lat}, {lon}")
                    else:
                        print(f"⚠️ No coordinates found for: {address}")

    if updated_count > 0:
        # Create backup
        backup_file = f"docs/data/backups/events_backup_rsoe_coords_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        print(f"💾 Creating backup: {backup_file}")

        with open(backup_file, 'w') as f:
            json.dump(events, f, indent=2)

        # Save updated events
        with open(events_file, 'w') as f:
            json.dump(events, f, indent=2)

        print(f"✅ Updated {updated_count} RSOE events with coordinates")
        print(f"💾 Saved to {events_file}")
    else:
        print("ℹ️ No RSOE events needed coordinate updates")

if __name__ == "__main__":
    main()