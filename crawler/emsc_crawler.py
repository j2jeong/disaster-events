import requests
import json
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import sys
import os
from typing import Dict, Any, List
from bs4 import BeautifulSoup

class EMSCCrawler:
    def __init__(self):
        self.base_url = "https://www.seismicportal.eu"
        # EMSC FDSN API endpoint
        self.api_url = "https://www.seismicportal.eu/fdsnws/event/1/query"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.collected_events = []

    def get_page_content(self, url, retries=2):
        """Fetch page content with retries"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=15)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                print(f"⚠️ Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(1)
                else:
                    print(f"✗ Failed to fetch {url} after {retries} attempts")
                    return None

    def crawl_from_main_page(self):
        """Crawl earthquake data from main earthquake page"""
        print("✓ Fetching EMSC main earthquake page...")

        content = self.get_page_content(self.earthquake_page_url)
        if not content:
            print("✗ Failed to fetch main earthquake page")
            return []

        soup = BeautifulSoup(content, 'html.parser')
        earthquakes = []

        # Look for earthquake data in tables or structured elements
        # EMSC typically shows latest earthquakes in a table format
        tables = soup.find_all('table')

        for table in tables:
            rows = table.find_all('tr')

            # Skip header row
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 6:  # Typical earthquake table has: Date, Time, Lat, Lon, Depth, Mag, Region
                    earthquake_data = self.parse_earthquake_row(cells)
                    if earthquake_data:
                        earthquakes.append(earthquake_data)

        # Also look for JSON data in script tags
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and ('earthquake' in script.string.lower() or 'quake' in script.string.lower()):
                json_data = self.extract_json_from_script(script.string)
                if json_data:
                    earthquakes.extend(json_data)

        return earthquakes

    def parse_earthquake_row(self, cells):
        """Parse a table row containing earthquake data"""
        try:
            if len(cells) < 6:
                return None

            # Typical format: Date, Time, Latitude, Longitude, Depth, Magnitude, Region
            date_cell = cells[0].get_text(strip=True)
            time_cell = cells[1].get_text(strip=True)
            lat_cell = cells[2].get_text(strip=True)
            lon_cell = cells[3].get_text(strip=True)
            depth_cell = cells[4].get_text(strip=True)
            mag_cell = cells[5].get_text(strip=True)
            region_cell = cells[6].get_text(strip=True) if len(cells) > 6 else ""

            # Clean and validate data
            latitude = self.clean_coordinate(lat_cell)
            longitude = self.clean_coordinate(lon_cell)
            magnitude = self.clean_magnitude(mag_cell)
            depth = self.clean_depth(depth_cell)

            if not all([latitude, longitude, magnitude]):
                return None

            # Combine date and time
            event_datetime = self.parse_datetime(date_cell, time_cell)
            if not event_datetime:
                return None

            # Generate unique event ID
            event_id = self.generate_earthquake_id(latitude, longitude, magnitude, event_datetime)

            return {
                "event_id": f"EMSC_{event_id}",
                "event_title": f"M{magnitude} earthquake - {region_cell}",
                "event_category": "Earthquake",
                "original_category": "Earthquake",
                "source": "EMSC",
                "source_url": self.earthquake_page_url,
                "event_date_utc": event_datetime,
                "last_update_utc": event_datetime,
                "latitude": latitude,
                "longitude": longitude,
                "area_range": "",
                "address": region_cell,
                "description": f"Magnitude {magnitude} earthquake at depth {depth}km",
                "magnitude": magnitude,
                "depth_km": depth,
                "crawled_at": datetime.now().isoformat(),
                "event_url": self.earthquake_page_url,
                "data_source": "emsc"
            }

        except Exception as e:
            print(f"⚠️ Error parsing earthquake row: {e}")
            return None

    def extract_json_from_script(self, script_content):
        """Extract earthquake data from JavaScript variables"""
        earthquakes = []
        try:
            # Look for common patterns in EMSC JavaScript
            # They often embed data in variables like 'quakes' or 'earthquakes'

            # Pattern for array of earthquake objects
            json_pattern = r'(?:quakes|earthquakes|events)\s*=\s*(\[.*?\]);'
            matches = re.findall(json_pattern, script_content, re.DOTALL)

            for match in matches:
                try:
                    data = json.loads(match)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                earthquake = self.parse_json_earthquake(item)
                                if earthquake:
                                    earthquakes.append(earthquake)
                except json.JSONDecodeError:
                    continue

        except Exception as e:
            print(f"⚠️ Error extracting JSON from script: {e}")

        return earthquakes

    def parse_json_earthquake(self, earthquake_json):
        """Parse earthquake data from JSON object"""
        try:
            # Common fields in EMSC JSON data
            lat = earthquake_json.get('lat') or earthquake_json.get('latitude')
            lon = earthquake_json.get('lon') or earthquake_json.get('longitude')
            mag = earthquake_json.get('mag') or earthquake_json.get('magnitude')
            depth = earthquake_json.get('depth')
            time_str = earthquake_json.get('time') or earthquake_json.get('datetime')
            region = earthquake_json.get('region') or earthquake_json.get('place', '')

            if not all([lat, lon, mag]):
                return None

            # Clean data
            latitude = str(float(lat))
            longitude = str(float(lon))
            magnitude = str(float(mag))
            depth_km = str(float(depth)) if depth else "0"

            # Parse time
            event_datetime = self.parse_json_datetime(time_str)
            if not event_datetime:
                return None

            # Generate ID
            event_id = self.generate_earthquake_id(latitude, longitude, magnitude, event_datetime)

            return {
                "event_id": f"EMSC_{event_id}",
                "event_title": f"M{magnitude} earthquake - {region}",
                "event_category": "Earthquake",
                "original_category": "Earthquake",
                "source": "EMSC",
                "source_url": self.earthquake_page_url,
                "event_date_utc": event_datetime,
                "last_update_utc": event_datetime,
                "latitude": latitude,
                "longitude": longitude,
                "area_range": "",
                "address": region,
                "description": f"Magnitude {magnitude} earthquake at depth {depth_km}km",
                "magnitude": magnitude,
                "depth_km": depth_km,
                "crawled_at": datetime.now().isoformat(),
                "event_url": self.earthquake_page_url,
                "data_source": "emsc"
            }

        except Exception as e:
            print(f"⚠️ Error parsing JSON earthquake: {e}")
            return None

    def clean_coordinate(self, coord_str):
        """Clean and validate coordinate string"""
        try:
            # Remove any non-numeric characters except decimal point and minus
            cleaned = re.sub(r'[^\d\.\-]', '', coord_str.strip())
            if cleaned:
                coord = float(cleaned)
                return str(coord)
        except ValueError:
            pass
        return None

    def clean_magnitude(self, mag_str):
        """Clean and validate magnitude string"""
        try:
            # Extract numeric value
            cleaned = re.sub(r'[^\d\.]', '', mag_str.strip())
            if cleaned:
                mag = float(cleaned)
                if 0 <= mag <= 10:  # Reasonable magnitude range
                    return str(mag)
        except ValueError:
            pass
        return None

    def clean_depth(self, depth_str):
        """Clean and validate depth string"""
        try:
            # Extract numeric value
            cleaned = re.sub(r'[^\d\.]', '', depth_str.strip())
            if cleaned:
                depth = float(cleaned)
                if 0 <= depth <= 1000:  # Reasonable depth range in km
                    return str(depth)
        except ValueError:
            pass
        return "0"

    def parse_datetime(self, date_str, time_str):
        """Parse date and time strings to ISO format"""
        try:
            # Combine date and time
            datetime_str = f"{date_str} {time_str}".strip()

            # Try common formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
                "%d-%m-%Y %H:%M:%S",
                "%d/%m/%Y %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y/%m/%d %H:%M",
                "%d-%m-%Y %H:%M",
                "%d/%m/%Y %H:%M"
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(datetime_str, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue

            # If no format works, try parsing just the date
            for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat()
                except ValueError:
                    continue

        except Exception:
            pass

        return None

    def parse_json_datetime(self, time_str):
        """Parse datetime from JSON format"""
        try:
            if not time_str:
                return None

            # Try ISO format first
            if 'T' in str(time_str):
                try:
                    dt = datetime.fromisoformat(str(time_str).replace('Z', '+00:00'))
                    return dt.isoformat()
                except:
                    pass

            # Try timestamp
            try:
                timestamp = float(time_str)
                if timestamp > 1000000000000:  # Milliseconds
                    timestamp = timestamp / 1000
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except:
                pass

            return None

        except Exception:
            return None

    def generate_earthquake_id(self, lat, lon, mag, datetime_str):
        """Generate unique ID for earthquake"""
        try:
            # Create ID from coordinates, magnitude, and date
            date_part = datetime_str[:10].replace('-', '')  # YYYYMMDD
            lat_part = str(abs(float(lat)))[:6].replace('.', '')
            lon_part = str(abs(float(lon)))[:6].replace('.', '')
            mag_part = str(float(mag)).replace('.', '')

            return f"{date_part}_{lat_part}_{lon_part}_{mag_part}"
        except:
            return str(hash(f"{lat}_{lon}_{mag}_{datetime_str}"))

    def crawl_earthquakes(self):
        """Main crawling method using EMSC FDSN API"""
        print("=== STARTING EMSC EARTHQUAKE CRAWLING ===")
        print("=" * 60)

        try:
            # Get earthquakes from last 7 days with magnitude >= 4.0
            earthquakes = self.fetch_earthquakes_api()

            if not earthquakes:
                print("✗ No earthquakes found")
                return False

            print(f"✓ Found {len(earthquakes)} earthquakes")

            self.collected_events = earthquakes
            print(f"✓ Collected {len(self.collected_events)} recent earthquakes")

            if self.collected_events:
                # Show magnitude distribution
                mag_ranges = {'M4-5': 0, 'M5-6': 0, 'M6+': 0}
                for eq in self.collected_events:
                    try:
                        mag = float(eq.get('magnitude', 0))
                        if mag >= 6:
                            mag_ranges['M6+'] += 1
                        elif mag >= 5:
                            mag_ranges['M5-6'] += 1
                        elif mag >= 4:
                            mag_ranges['M4-5'] += 1
                    except:
                        pass

                print("\nEarthquakes by magnitude:")
                for range_name, count in mag_ranges.items():
                    if count > 0:
                        print(f"  {range_name}: {count}")

            print("=" * 60)
            return True

        except Exception as e:
            print(f"✗ Error during EMSC crawling: {e}")
            import traceback
            traceback.print_exc()
            return False

    def fetch_earthquakes_api(self):
        """Fetch earthquakes using EMSC FDSN API"""
        print("✓ Fetching earthquakes from EMSC FDSN API...")

        # Get data from last 7 days
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)

        params = {
            'format': 'json',
            'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': '4.0',  # Only significant earthquakes
            'limit': '100'  # Limit to avoid too much data
        }

        try:
            response = self.session.get(self.api_url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if 'features' not in data:
                print("✗ No earthquake features found in API response")
                return []

            earthquakes = []
            for feature in data['features']:
                earthquake = self.parse_api_earthquake(feature)
                if earthquake:
                    earthquakes.append(earthquake)

            return earthquakes

        except Exception as e:
            print(f"✗ Error fetching from EMSC API: {e}")
            return []

    def parse_api_earthquake(self, feature):
        """Parse earthquake data from EMSC FDSN API response"""
        try:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [])

            if len(coordinates) < 3:
                return None

            longitude = coordinates[0]
            latitude = coordinates[1]
            depth = coordinates[2] if len(coordinates) > 2 else 0

            magnitude = properties.get('mag', 0)
            time_str = properties.get('time')
            place = properties.get('place', '')
            event_id = properties.get('id', '')

            # Convert time from milliseconds timestamp
            if time_str:
                try:
                    if isinstance(time_str, str):
                        event_datetime = time_str
                    else:
                        # Assume it's milliseconds timestamp
                        event_datetime = datetime.fromtimestamp(time_str / 1000).isoformat()
                except:
                    event_datetime = datetime.now().isoformat()
            else:
                event_datetime = datetime.now().isoformat()

            # Generate unique ID
            unique_id = self.generate_earthquake_id(str(latitude), str(longitude), str(magnitude), event_datetime)

            return {
                "event_id": f"EMSC_{unique_id}",
                "event_title": f"M{magnitude} earthquake - {place}",
                "event_category": "Earthquake",
                "original_category": "Earthquake",
                "source": "EMSC",
                "source_url": "https://www.emsc-csem.org/",
                "event_date_utc": event_datetime,
                "last_update_utc": event_datetime,
                "latitude": str(latitude),
                "longitude": str(longitude),
                "area_range": "",
                "address": place,
                "description": f"Magnitude {magnitude} earthquake at depth {depth}km",
                "magnitude": str(magnitude),
                "depth_km": str(depth),
                "crawled_at": datetime.now().isoformat(),
                "event_url": "https://www.emsc-csem.org/",
                "data_source": "emsc"
            }

        except Exception as e:
            print(f"⚠️ Error parsing API earthquake: {e}")
            return None

    def get_events(self):
        """Return collected events"""
        return self.collected_events


def main():
    """Test the EMSC crawler"""
    print("🌍 Testing EMSC Earthquake Crawler")
    print("=" * 40)

    crawler = EMSCCrawler()
    success = crawler.crawl_earthquakes()

    if success:
        events = crawler.get_events()
        print(f"\n✓ Successfully collected {len(events)} earthquakes")

        # Show first few earthquakes
        for i, event in enumerate(events[:3], 1):
            print(f"\n[{i}] {event['event_title']}")
            print(f"    Location: {event['latitude']}, {event['longitude']}")
            print(f"    Magnitude: {event.get('magnitude', 'N/A')}")
            print(f"    Date: {event['event_date_utc']}")
    else:
        print("\n✗ Crawling failed")


if __name__ == "__main__":
    main()