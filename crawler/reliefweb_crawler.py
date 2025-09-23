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
        self.rss_url = "https://reliefweb.int/disasters/rss.xml"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.collected_events = []

        # ReliefWeb disaster type mapping to our categories
        # Maps all 22 ReliefWeb disaster types to 9 RSOE standard categories
        self.disaster_type_mapping = {
            # Direct mappings
            'earthquake': 'Earthquake',
            'flood': 'Flood',
            'flash flood': 'Flood',
            'fire': 'Fire in built environment',
            'wild fire': 'Fire in built environment',
            'landslide': 'Landslide',
            'land slide': 'Landslide',
            'mud slide': 'Landslide',
            'volcano': 'Volcanic eruption',
            'volcanic': 'Volcanic eruption',

            # Water-related disasters -> Flood
            'drought': 'Flood',
            'tsunami': 'Flood',
            'storm surge': 'Flood',
            'extratropical cyclone': 'Flood',
            'tropical cyclone': 'Flood',
            'cyclone': 'Flood',
            'hurricane': 'Flood',
            'typhoon': 'Flood',

            # Weather-related -> Environment pollution (closest match)
            'cold wave': 'Environment pollution',
            'heat wave': 'Environment pollution',
            'severe local storm': 'Environment pollution',
            'snow avalanche': 'Environment pollution',

            # Health/Bio disasters -> Environment pollution
            'epidemic': 'Environment pollution',
            'insect infestation': 'Environment pollution',

            # Human-caused disasters
            'complex emergency': 'War',
            'war': 'War',
            'conflict': 'War',
            'technological disaster': 'Industrial explosion',
            'explosion': 'Industrial explosion',
            'industrial accident': 'Industrial explosion',

            # Catch-all
            'other': 'Environment pollution'
        }

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

            # Map disaster type to our categories
            mapped_category = self.map_disaster_type(disaster_type)
            if not mapped_category:
                return None  # Skip if we don't handle this type

            # Extract additional details from description
            glide_number = self.extract_glide_number(description_text)

            # Parse date
            event_date = self.parse_date(pub_date_text)

            # Generate unique ID from URL
            event_id = self.generate_event_id(link_url)

            return {
                "event_id": f"RW_{event_id}",
                "event_title": title_text.strip(),
                "event_category": mapped_category,
                "original_category": disaster_type,
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
        """Extract country and disaster type from title"""
        # Format: "Country: Disaster Type - Date"
        try:
            if ':' in title:
                country_part, rest = title.split(':', 1)
                country = country_part.strip()

                if '-' in rest:
                    disaster_part = rest.split('-')[0].strip()
                else:
                    disaster_part = rest.strip()

                return country, disaster_part.lower()
            else:
                return "", title.lower()
        except:
            return "", title.lower()

    def map_disaster_type(self, disaster_type):
        """Map ReliefWeb disaster type to our standard categories"""
        disaster_type = disaster_type.lower().strip()

        # Try exact matching first
        if disaster_type in self.disaster_type_mapping:
            return self.disaster_type_mapping[disaster_type]

        # Try partial matching for compound disaster types
        for key, mapped in self.disaster_type_mapping.items():
            if key in disaster_type:
                return mapped

        # Additional keyword matching for variations
        keyword_mapping = {
            'fire': ['fire', 'burn', 'blaze', 'wildfire'],
            'flood': ['flood', 'inundation', 'deluge', 'storm', 'cyclone', 'hurricane', 'typhoon'],
            'earthquake': ['quake', 'seismic', 'tremor'],
            'landslide': ['slide', 'landslip', 'rockfall', 'mudslide'],
            'volcano': ['erupt', 'volcano', 'volcanic', 'lava'],
            'war': ['war', 'conflict', 'violence', 'emergency', 'crisis'],
            'pollution': ['pollut', 'contamin', 'toxic', 'chemical', 'epidemic', 'disease'],
            'explosion': ['explos', 'blast', 'detona', 'accident', 'technological']
        }

        for disaster_key, keywords in keyword_mapping.items():
            if any(keyword in disaster_type for keyword in keywords):
                # Map to appropriate category
                if disaster_key == 'fire':
                    return 'Fire in built environment'
                elif disaster_key == 'flood':
                    return 'Flood'
                elif disaster_key == 'earthquake':
                    return 'Earthquake'
                elif disaster_key == 'landslide':
                    return 'Landslide'
                elif disaster_key == 'volcano':
                    return 'Volcanic eruption'
                elif disaster_key == 'war':
                    return 'War'
                elif disaster_key == 'pollution':
                    return 'Environment pollution'
                elif disaster_key == 'explosion':
                    return 'Industrial explosion'

        return None  # Don't collect if we can't categorize

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
        """Main crawling method"""
        print("=== STARTING RELIEFWEB DISASTER CRAWLING ===")
        print("=" * 60)

        try:
            disasters = self.parse_rss_feed()

            if not disasters:
                print("✗ No disasters found in RSS feed")
                return False

            print(f"✓ Processing {len(disasters)} disaster reports...")

            for disaster in disasters:
                if disaster['event_category']:  # Only collect categorized disasters
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