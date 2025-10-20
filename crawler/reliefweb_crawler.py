import requests
import xml.etree.ElementTree as ET
import json
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import sys
import os
from typing import Dict, Any, List
from bs4 import BeautifulSoup

class ReliefWebCrawler:
    def __init__(self):
        self.api_url = "https://api.reliefweb.int/v2/disasters"
        self.rss_url = "https://reliefweb.int/disasters/rss.xml"  # Keep RSS as backup
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.collected_events = []

        # Use ReliefWeb-provided categories as-is (no custom mapping)

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

    def parse_rss_feed(self):
        """Parse ReliefWeb RSS feed for disaster information"""
        print("✓ Fetching ReliefWeb RSS feed...")

        content = self.get_page_content(self.rss_url)
        if not content:
            print("✗ Failed to fetch RSS feed")
            return []

        try:
            root = ET.fromstring(content)
            items = root.findall('.//item')
            print(f"✓ Found {len(items)} items in RSS feed")

            disasters = []
            for item in items:
                disaster_data = self.parse_rss_item(item)
                if disaster_data:
                    disasters.append(disaster_data)

            return disasters

        except ET.ParseError as e:
            print(f"✗ Error parsing RSS XML: {e}")
            return []

    def parse_rss_item(self, item):
        """Parse individual RSS item to extract disaster data"""
        try:
            title = item.find('title')
            title_text = title.text if title is not None else ""

            link = item.find('link')
            link_url = link.text if link is not None else ""

            pub_date = item.find('pubDate')
            pub_date_text = pub_date.text if pub_date is not None else ""

            description = item.find('description')
            description_text = description.text if description is not None else ""

            # Extract country and disaster type from title
            # Format usually: "Country: Disaster Type - Date"
            country, disaster_type = self.extract_location_and_type(title_text)
            event_category = disaster_type.strip() if disaster_type else ''

            # Extract additional details from description
            glide_number = self.extract_glide_number(description_text)

            # Parse date
            event_date = self.parse_date(pub_date_text)

            # Generate unique ID from URL
            event_id = self.generate_event_id(link_url)

            return {
                "event_id": f"RW_{event_id}",
                "event_title": title_text.strip(),
                "event_category": event_category,
                "original_category": event_category,
                "source": "ReliefWeb",
                "source_url": link_url,
                "event_date_utc": event_date,
                "last_update_utc": event_date,
                "latitude": "",  # Will be extracted from detail page if needed
                "longitude": "",
                "area_range": "",
                "address": country,
                "description": description_text[:500] + "..." if len(description_text) > 500 else description_text,
                "glide_number": glide_number,
                "crawled_at": datetime.now().isoformat(),
                "event_url": link_url,
                "data_source": "reliefweb"
            }

        except Exception as e:
            print(f"⚠️ Error parsing RSS item: {e}")
            return None

    def extract_location_and_type(self, title):
        """Extract country and disaster type from title, preserving original case"""
        # Format: "Country: Disaster Type - Date"
        try:
            if ':' in title:
                country_part, rest = title.split(':', 1)
                country = country_part.strip()

                if '-' in rest:
                    disaster_part = rest.split('-')[0].strip()
                else:
                    disaster_part = rest.strip()

                return country, disaster_part
            else:
                return "", title
        except:
            return "", title

    # NOTE: Removed custom disaster type mapping; we keep ReliefWeb categories verbatim.

    def extract_glide_number(self, description):
        """Extract GLIDE number from description if present"""
        # GLIDE format: XX-YYYY-NNNNNN-CCC
        glide_pattern = r'[A-Z]{2}-\d{4}-\d{6}-[A-Z]{3}'
        match = re.search(glide_pattern, description)
        return match.group(0) if match else ""

    def parse_date(self, date_string):
        """Parse RSS date string to ISO format"""
        try:
            if not date_string:
                return datetime.now().isoformat()

            # RSS dates are usually in RFC 2822 format
            # Example: "Wed, 02 Oct 2002 08:00:00 EST"
            from email.utils import parsedate_tz, mktime_tz
            parsed = parsedate_tz(date_string)
            if parsed:
                timestamp = mktime_tz(parsed)
                return datetime.fromtimestamp(timestamp).isoformat()
            else:
                return datetime.now().isoformat()
        except:
            return datetime.now().isoformat()

    def generate_event_id(self, url):
        """Generate unique event ID from URL"""
        try:
            # Extract the disaster identifier from URL
            # Example: https://reliefweb.int/disaster/fl-2024-000123-afg
            path = urlparse(url).path
            if '/disaster/' in path:
                disaster_id = path.split('/disaster/')[-1]
                return disaster_id.replace('/', '_')
            else:
                # Fallback: use hash of URL
                return str(hash(url))
        except:
            return str(hash(url))

    def crawl_disasters(self):
        """Main crawling method using ReliefWeb API"""
        print("=== STARTING RELIEFWEB DISASTER CRAWLING ===")
        print("=" * 60)

        try:
            disasters = self.fetch_disasters_api()

            if not disasters:
                print("✗ No disasters found via API")
                return False

            print(f"✓ Processing {len(disasters)} disaster reports from API...")

            for disaster in disasters:
                if disaster and disaster.get('event_category'):  # Only collect categorized disasters
                    self.collected_events.append(disaster)

            print(f"✓ Collected {len(self.collected_events)} relevant disasters")

            if self.collected_events:
                category_stats = {}
                for event in self.collected_events:
                    cat = event['event_category']
                    category_stats[cat] = category_stats.get(cat, 0) + 1

                print("\nReliefWeb events by category:")
                for cat, count in sorted(category_stats.items()):
                    print(f"  {cat}: {count}")

            print("=" * 60)
            return True

        except Exception as e:
            print(f"✗ Error during ReliefWeb crawling: {e}")
            import traceback
            traceback.print_exc()
            return False

    def fetch_disasters_api(self):
        """Fetch disasters using ReliefWeb API"""
        print("✓ Fetching disasters from ReliefWeb API...")

        # Build POST payload per ReliefWeb API v2 conventions
        recent_from = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%dT00:00:00+00:00')
        payload = {
            "appname": "multi-source-disaster-crawler.github.io",
            "limit": 100,
            "sort": ["date.created:desc"],
            "filter": {
                "field": "date.created",
                "value": {"from": recent_from}
            },
            "fields": {
                # Request fields that let us build a proper disaster URL and country/type safely
                "include": [
                    "name",
                    "type",
                    "primary_type",
                    "date",
                    "country",
                    "primary_country",
                    "glide",
                    "url_alias",
                    "url",
                    "description",
                    "body"
                ]
            }
        }

        try:
            print("✓ Making API request to ReliefWeb v2 (POST)...")
            response = self.session.post(self.api_url, json=payload, timeout=30)

            print(f"✓ Response status: {response.status_code}")

            if response.status_code != 200:
                print(f"✗ API returned status {response.status_code}: {response.text[:200]}")
                raise Exception(f"API returned status {response.status_code}")

            data = response.json()

            if 'data' not in data:
                print("✗ No disaster data found in API response")
                print(f"Available keys: {list(data.keys())}")
                return []

            print(f"✓ Found {len(data['data'])} disasters from API")

            disasters = []
            for disaster_data in data['data']:
                disaster = self.parse_api_disaster(disaster_data)
                if disaster:
                    disasters.append(disaster)

            return disasters

        except Exception as e:
            print(f"✗ Error fetching from ReliefWeb API: {e}")
            # Fallback to RSS if API fails
            print("⚠️ Falling back to RSS feed...")
            return self.parse_rss_feed()

    def parse_api_disaster(self, disaster_data):
        """Parse disaster data from ReliefWeb API response"""
        try:
            fields = disaster_data.get('fields', {})

            disaster_id = disaster_data.get('id', '')

            # Handle different field structures
            name = fields.get('name', '') or fields.get('title', '')

            # Type can be in different formats; prefer primary_type when available
            disaster_types = fields.get('primary_type') or fields.get('type', [])
            type_name = ''

            if isinstance(disaster_types, list) and disaster_types:
                # Handle list of type objects or strings
                first_type = disaster_types[0]
                if isinstance(first_type, dict):
                    type_name = first_type.get('name', '') or first_type.get('title', '')
                else:
                    type_name = str(first_type)
            elif isinstance(disaster_types, dict):
                type_name = disaster_types.get('name', '') or disaster_types.get('title', '')
            elif isinstance(disaster_types, str):
                type_name = disaster_types

            # Skip if no disaster name (type can be empty on rare cases)
            if not name:
                print(f"⚠️ Skipping disaster - missing name or type: {disaster_data}")
                return None

            # Use ReliefWeb type as-is for event category
            event_category = type_name

            # Extract country information; prefer primary_country when available
            countries = fields.get('primary_country') or fields.get('country', [])
            country_name = ''

            if isinstance(countries, list) and countries:
                first_country = countries[0]
                if isinstance(first_country, dict):
                    country_name = first_country.get('name', '') or first_country.get('title', '')
                else:
                    country_name = str(first_country)
            elif isinstance(countries, dict):
                country_name = countries.get('name', '') or countries.get('title', '')
            elif isinstance(countries, str):
                country_name = countries

            # Extract date information
            date_info = fields.get('date', {})
            event_date = datetime.now().isoformat()

            if isinstance(date_info, dict):
                # Try different date fields
                for date_field in ['event', 'created', 'changed']:
                    if date_field in date_info:
                        try:
                            event_date = date_info[date_field]
                            if not event_date.endswith('Z') and 'T' in event_date:
                                event_date = event_date + 'Z'
                            break
                        except:
                            continue
            elif isinstance(date_info, str):
                event_date = date_info

            # Get additional fields
            glide = fields.get('glide', '') or ''

            # ReliefWeb disaster URLs follow: https://reliefweb.int/disaster/{disaster-code}
            # Prefer url_alias which directly contains the path like 'disaster/fl-2024-000123-afg'
            url_alias = fields.get('url_alias', '') or ''
            api_url = fields.get('url', '') or ''

            # Build canonical event URL
            if url_alias:
                # Ensure leading slash not duplicated
                alias = url_alias.lstrip('/')
                url = f"https://reliefweb.int/{alias}"
            elif api_url:
                # Some APIs return taxonomy term URLs; keep as fallback
                url = api_url
            else:
                # Fallback to best-guess path using numeric id (not ideal but stable)
                url = f"https://reliefweb.int/taxonomy/term/{disaster_id}"
            description = fields.get('description', '') or fields.get('body', '') or name

            # Clean up description
            if description and len(description) > 300:
                # Remove HTML tags if present
                try:
                    from bs4 import BeautifulSoup
                    clean_description = BeautifulSoup(description, 'html.parser').get_text()
                    if len(clean_description) > 300:
                        clean_description = clean_description[:300] + "..."
                    description = clean_description
                except:
                    description = description[:300] + "..."

            # Coordinates: try to extract from the ReliefWeb disaster page; otherwise leave blank
            latitude, longitude = "", ""
            if url and '/disaster/' in url:
                lat_try, lon_try = self.extract_coordinates_from_url(url)
                if lat_try and lon_try:
                    latitude, longitude = lat_try, lon_try
            if not (latitude and longitude):
                print(f"ℹ️ No in-page coordinates; will rely on dynamic geocoder for: {country_name or name}")

            # TODO: In the future, we could extract coordinates from ReliefWeb's map API
            # or from the main disasters listing page which might contain coordinate data

            # Use disaster code from url_alias for stable IDs when available
            event_code = None
            if url_alias and '/disaster/' in url_alias:
                try:
                    event_code = url_alias.split('/disaster/')[-1].strip('/').split('/')[0]
                except Exception:
                    event_code = None

            return {
                "event_id": f"RW_{event_code}" if event_code else f"RW_{disaster_id}",
                "event_title": f"{country_name}: {name}" if country_name else name,
                "event_category": event_category,
                "original_category": type_name,
                "source": "ReliefWeb",
                "source_url": url,
                "event_date_utc": event_date,
                "last_update_utc": event_date,
                "latitude": latitude,
                "longitude": longitude,
                "area_range": "",
                "address": country_name,
                "description": description,
                "glide_number": glide,
                "crawled_at": datetime.now().isoformat(),
                "event_url": url,
                "data_source": "reliefweb"
            }

        except Exception as e:
            print(f"⚠️ Error parsing API disaster: {e}")
            import traceback
            traceback.print_exc()
            return None

    # NOTE: Removed hardcoded country coordinate fallback and normalization helper per request.

    def extract_coordinates_from_url(self, url):
        """Extract coordinates from ReliefWeb disaster page"""
        try:
            if not url or '/disaster/' not in url:
                return "", ""

            print(f"🌍 Extracting coordinates from: {url}")
            content = self.get_page_content(url, retries=2)
            if not content:
                print(f"⚠️ Failed to fetch page content from {url}")
                return "", ""

            # Look for data-disaster-lat and data-disaster-lon attributes using regex
            import re
            lat_pattern = r'data-disaster-lat=["\']([^"\']+)["\']'
            lon_pattern = r'data-disaster-lon=["\']([^"\']+)["\']'

            lat_match = re.search(lat_pattern, content)
            lon_match = re.search(lon_pattern, content)

            if lat_match and lon_match:
                lat = lat_match.group(1).strip()
                lon = lon_match.group(1).strip()
                print(f"✅ Found coordinates via regex: lat={lat}, lon={lon}")
                return lat, lon

            # Alternative: Look for BeautifulSoup parsing
            try:
                soup = BeautifulSoup(content, 'html.parser')
                disaster_articles = soup.find_all('article', attrs={
                    'data-disaster-lat': True,
                    'data-disaster-lon': True
                })

                if disaster_articles:
                    article = disaster_articles[0]
                    lat = article.get('data-disaster-lat', '').strip()
                    lon = article.get('data-disaster-lon', '').strip()

                    if lat and lon:
                        print(f"✅ Found coordinates via BeautifulSoup: lat={lat}, lon={lon}")
                        return lat, lon
            except Exception as soup_error:
                print(f"⚠️ BeautifulSoup parsing failed: {soup_error}")

            print(f"⚠️ No coordinates found in {url}")
            return "", ""

        except Exception as e:
            print(f"⚠️ Error extracting coordinates from {url}: {e}")
            return "", ""

    def get_events(self):
        """Return collected events"""
        return self.collected_events


def main():
    """Test the ReliefWeb crawler"""
    print("🌍 Testing ReliefWeb Crawler")
    print("=" * 40)

    crawler = ReliefWebCrawler()
    success = crawler.crawl_disasters()

    if success:
        events = crawler.get_events()
        print(f"\n✓ Successfully collected {len(events)} events")

        # Show first few events
        for i, event in enumerate(events[:3], 1):
            print(f"\n[{i}] {event['event_title']}")
            print(f"    Category: {event['event_category']}")
            print(f"    Location: {event['address']}")
            print(f"    Date: {event['event_date_utc']}")
    else:
        print("\n✗ Crawling failed")


if __name__ == "__main__":
    main()
