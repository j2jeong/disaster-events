#!/usr/bin/env python3
"""
Update ReliefWeb events with missing coordinates using country mappings
"""
import json
import sys
import os
from datetime import datetime

# Add crawler directory to path to import ReliefWebCrawler
sys.path.append(os.path.join(os.path.dirname(__file__), 'crawler'))

from reliefweb_crawler import ReliefWebCrawler

def main():
    print("🔄 Updating ReliefWeb coordinates in existing data...")

    # Load current events
    events_file = 'docs/data/events.json'
    with open(events_file, 'r') as f:
        events = json.load(f)

    print(f"📊 Total events loaded: {len(events)}")

    # Find ReliefWeb events with missing coordinates
    crawler = ReliefWebCrawler()
    updated_count = 0

    for event in events:
        if event.get('data_source') == 'reliefweb':
            current_lat = event.get('latitude', '')
            current_lon = event.get('longitude', '')

            # Check if coordinates are missing or zero
            try:
                lat_float = float(current_lat) if current_lat not in ['', None] else 0
                lon_float = float(current_lon) if current_lon not in ['', None] else 0
                needs_update = (lat_float == 0 or lon_float == 0)
            except:
                needs_update = True

            if needs_update:
                country_name = event.get('address', '')
                if country_name:
                    lat, lon = crawler.get_country_coordinates(country_name)
                    if lat and lon:
                        event['latitude'] = lat
                        event['longitude'] = lon
                        updated_count += 1
                        print(f"✅ Updated {event['event_id']}: {country_name} -> {lat}, {lon}")
                    else:
                        print(f"⚠️ No coordinates found for: {country_name}")

    if updated_count > 0:
        # Create backup
        backup_file = f"docs/data/backups/events_backup_coordinate_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        print(f"💾 Creating backup: {backup_file}")

        os.makedirs(os.path.dirname(backup_file), exist_ok=True)
        with open(backup_file, 'w') as f:
            json.dump(events, f, indent=2)

        # Save updated events
        with open(events_file, 'w') as f:
            json.dump(events, f, indent=2)

        print(f"✅ Updated {updated_count} ReliefWeb events with coordinates")
        print(f"💾 Saved to {events_file}")
    else:
        print("ℹ️ No events needed coordinate updates")

if __name__ == "__main__":
    main()