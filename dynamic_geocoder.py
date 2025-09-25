#!/usr/bin/env python3
"""
Dynamic geocoding solution for missing coordinates
Uses multiple free geocoding services without hardcoded mappings
"""
import json
import time
import re
import requests
from typing import Tuple, Optional
from datetime import datetime


class DynamicGeocoder:
    """
    Dynamic geocoder using multiple free services
    """

    def __init__(self):
        self.cache = {}  # Simple in-memory cache
        self.request_count = 0
        self.max_requests_per_minute = 50  # Conservative limit

    def normalize_address(self, address: str) -> str:
        """Normalize address for better matching"""
        if not address or address.strip() == "-":
            return ""

        # Clean up common patterns
        address = re.sub(r'\s+', ' ', address.strip())
        address = re.sub(r'[,;]+', ',', address)

        # Extract meaningful location parts
        # Remove technical details but keep location names
        parts = []
        for part in address.split(','):
            part = part.strip()
            if part and not re.match(r'^[\d\s\-\.]+$', part):  # Skip pure numbers/coordinates
                parts.append(part)

        return ', '.join(parts[:3])  # Take first 3 meaningful parts

    def geocode_with_nominatim(self, address: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Use OpenStreetMap Nominatim for geocoding
        Free service with reasonable rate limits
        """
        try:
            url = "https://nominatim.openstreetmap.org/search"
            params = {
                'q': address,
                'format': 'json',
                'limit': 1,
                'addressdetails': 1
            }

            # Add User-Agent header as required by Nominatim
            headers = {
                'User-Agent': 'DisasterMonitor/1.0 (disaster-monitoring-system)'
            }

            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data and len(data) > 0:
                result = data[0]
                lat = result.get('lat')
                lon = result.get('lon')

                if lat and lon:
                    # Validate coordinates
                    try:
                        lat_f = float(lat)
                        lon_f = float(lon)
                        if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                            return str(lat_f), str(lon_f)
                    except ValueError:
                        pass

            return None, None

        except Exception as e:
            print(f"Nominatim geocoding failed for '{address}': {e}")
            return None, None

    def extract_coordinates_from_text(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract coordinates directly from text if available
        Look for patterns like "Latitude: 12.34" or "12.34, -56.78"
        """
        if not text:
            return None, None

        # Pattern 1: "Latitude: 12.34" format
        lat_match = re.search(r'latitude[:\s]+([-\d\.]+)', text, re.IGNORECASE)
        lon_match = re.search(r'longitude[:\s]+([-\d\.]+)', text, re.IGNORECASE)

        if lat_match and lon_match:
            try:
                lat = float(lat_match.group(1))
                lon = float(lon_match.group(1))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return str(lat), str(lon)
            except ValueError:
                pass

        # Pattern 2: "12.34, -56.78" format
        coord_pattern = r'([-\d\.]+)[,\s]+([-\d\.]+)'
        matches = re.findall(coord_pattern, text)

        for match in matches:
            try:
                lat, lon = float(match[0]), float(match[1])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return str(lat), str(lon)
            except ValueError:
                continue

        return None, None

    def get_coordinates(self, address: str, event_data: dict = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Main method to get coordinates for an address
        Uses multiple strategies in order of preference
        """
        if not address or address.strip() == "-":
            return None, None

        # Strategy 1: Check if coordinates are already in the event data text
        if event_data:
            # Check in last_update_utc field which sometimes contains coordinates
            last_update = event_data.get('last_update_utc', '')
            if 'latitude' in last_update.lower():
                lat, lon = self.extract_coordinates_from_text(last_update)
                if lat and lon:
                    return lat, lon

            # Check in event title
            title = event_data.get('event_title', '')
            lat, lon = self.extract_coordinates_from_text(title)
            if lat and lon:
                return lat, lon

        # Strategy 2: Use cache
        cache_key = address.lower().strip()
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Strategy 3: Normalize and geocode
        normalized_address = self.normalize_address(address)
        if not normalized_address:
            return None, None

        # Rate limiting
        if self.request_count >= self.max_requests_per_minute:
            print(f"⚠️ Rate limit reached, skipping geocoding for: {address}")
            return None, None

        # Try geocoding
        lat, lon = self.geocode_with_nominatim(normalized_address)
        self.request_count += 1

        # Cache result (even if None)
        self.cache[cache_key] = (lat, lon)

        if lat and lon:
            print(f"✅ Geocoded '{address}' -> {lat}, {lon}")
        else:
            print(f"⚠️ Could not geocode: {address}")

        # Small delay to be respectful to the service
        time.sleep(0.1)

        return lat, lon


def update_events_with_geocoding(events_file: str = 'docs/data/events.json'):
    """
    Update events with missing coordinates using dynamic geocoding
    """
    print("🔄 Starting dynamic geocoding for missing coordinates...")

    # Load events
    try:
        with open(events_file, 'r') as f:
            events = json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load {events_file}: {e}")
        return

    print(f"📊 Total events loaded: {len(events)}")

    geocoder = DynamicGeocoder()
    updated_count = 0

    for event in events:
        # Focus on events with missing or zero coordinates
        current_lat = str(event.get('latitude', '')).strip()
        current_lon = str(event.get('longitude', '')).strip()

        def is_missing_coordinate(coord_str):
            if not coord_str or coord_str in ['', '0', '0.0', '0.00']:
                return True
            try:
                coord_val = float(coord_str)
                return coord_val == 0.0
            except:
                return True

        if is_missing_coordinate(current_lat) or is_missing_coordinate(current_lon):
            address = event.get('address', '')
            if address and address != '-':
                lat, lon = geocoder.get_coordinates(address, event)
                if lat and lon:
                    event['latitude'] = lat
                    event['longitude'] = lon
                    updated_count += 1
                    print(f"✅ Updated {event.get('event_id')}: {address} -> {lat}, {lon}")

    if updated_count > 0:
        # Create backup
        backup_file = f"{events_file}.backup_geocoded_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        try:
            with open(backup_file, 'w') as f:
                json.dump(events, f, indent=2)

            # Save updated events
            with open(events_file, 'w') as f:
                json.dump(events, f, indent=2)

            print(f"✅ Updated {updated_count} events with dynamic geocoding")
            print(f"💾 Backup saved to: {backup_file}")

        except Exception as e:
            print(f"⚠️ Failed to save updates: {e}")
    else:
        print("ℹ️ No events needed coordinate updates")


if __name__ == "__main__":
    update_events_with_geocoding()