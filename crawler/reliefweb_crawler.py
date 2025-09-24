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

        # Get recent disasters (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        # Use correct parameters for API v2 with required fields and recent filter
        params = {
            'appname': 'multi-source-disaster-crawler.github.io',
            'limit': 100,
            'sort[]': 'date.created:desc',  # Sort by creation date descending
            'fields[include][]': ['name', 'type', 'date', 'country', 'glide', 'url', 'description']
        }

        # Add date filter for recent disasters (last 60 days)
        recent_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        params['filter[field]'] = 'date.created'
        params['filter[value][from]'] = f"{recent_date}T00:00:00+00:00"

        try:
            print(f"✓ Making API request to ReliefWeb v2...")
            response = self.session.get(self.api_url, params=params, timeout=30)

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

            # Type can be in different formats
            disaster_types = fields.get('type', [])
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

            # Skip if no disaster type or name
            if not name or not type_name:
                print(f"⚠️ Skipping disaster - missing name or type: {disaster_data}")
                return None

            # Map disaster type to our categories
            mapped_category = self.map_disaster_type(type_name)
            if not mapped_category:
                print(f"⚠️ Skipping disaster - unmapped type '{type_name}'")
                return None

            # Extract country information
            countries = fields.get('country', [])
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

            # ReliefWeb disaster URLs follow the pattern: https://reliefweb.int/disaster/{disaster-code}
            # We need to construct the proper disaster URL, not the taxonomy URL
            api_url = fields.get('url', '')
            if api_url and '/taxonomy/term/' in api_url:
                # Extract the term ID and construct proper disaster URL
                term_id = api_url.split('/taxonomy/term/')[-1]
                url = f"https://reliefweb.int/disaster/{disaster_id}"
            else:
                url = api_url or f"https://reliefweb.int/disaster/{disaster_id}"
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

            # For now, use country coordinates as ReliefWeb individual disaster pages
            # don't seem to contain the coordinate data we need
            latitude, longitude = self.get_country_coordinates(country_name)
            if latitude and longitude:
                print(f"✅ Using country coordinates for {country_name}: {latitude}, {longitude}")
            else:
                print(f"⚠️ No coordinates found for country: {country_name}")

            # TODO: In the future, we could extract coordinates from ReliefWeb's map API
            # or from the main disasters listing page which might contain coordinate data

            return {
                "event_id": f"RW_{disaster_id}",
                "event_title": f"{country_name}: {name}" if country_name else name,
                "event_category": mapped_category,
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

    def get_country_coordinates(self, country_name):
        """Get approximate coordinates for a country"""
        if not country_name:
            return "", ""

        # Country center coordinates (approximate)
        country_coords = {
            'afghanistan': ('33.93', '67.71'),
            'albania': ('41.15', '20.17'),
            'algeria': ('28.03', '1.66'),
            'angola': ('-11.20', '17.87'),
            'argentina': ('-38.42', '-63.62'),
            'australia': ('-25.27', '133.78'),
            'bangladesh': ('23.68', '90.36'),
            'bolivia': ('-16.29', '-63.59'),
            'brazil': ('-14.24', '-51.93'),
            'cameroon': ('7.37', '12.35'),
            'canada': ('56.13', '-106.35'),
            'chad': ('15.45', '18.73'),
            'chile': ('-35.68', '-71.54'),
            'china': ('35.86', '104.20'),
            'colombia': ('4.57', '-74.30'),
            'democratic republic of the congo': ('-4.04', '21.76'),
            'ecuador': ('-1.83', '-78.18'),
            'egypt': ('26.82', '30.80'),
            'ethiopia': ('9.15', '40.49'),
            'ghana': ('7.95', '-1.02'),
            'guatemala': ('15.78', '-90.23'),
            'haiti': ('18.97', '-72.29'),
            'india': ('20.59', '78.96'),
            'indonesia': ('-0.79', '113.92'),
            'iran': ('32.43', '53.69'),
            'iraq': ('33.22', '43.68'),
            'kenya': ('-0.02', '37.91'),
            'madagascar': ('-18.77', '46.87'),
            'mali': ('17.57', '-3.99'),
            'mexico': ('23.63', '-102.55'),
            'mozambique': ('-18.67', '35.53'),
            'myanmar': ('21.91', '95.96'),
            'nepal': ('28.39', '84.12'),
            'niger': ('17.61', '8.08'),
            'nigeria': ('9.08', '8.68'),
            'pakistan': ('30.38', '69.35'),
            'peru': ('-9.19', '-75.02'),
            'philippines': ('12.88', '121.77'),
            'somalia': ('5.15', '46.20'),
            'south africa': ('-30.56', '22.94'),
            'south sudan': ('6.88', '31.31'),
            'sudan': ('12.86', '30.22'),
            'syria': ('34.80', '38.99'),
            'tanzania': ('-6.37', '34.89'),
            'turkey': ('38.96', '35.24'),
            'uganda': ('1.37', '32.29'),
            'ukraine': ('48.38', '31.17'),
            'venezuela': ('6.42', '-66.59'),
            'yemen': ('15.55', '48.52'),
            'zimbabwe': ('-19.02', '29.15'),
            'zambia': ('-13.13', '27.85'),

            # Additional missing countries
            'benin': ('9.31', '2.32'),
            'sierra leone': ('8.46', '-11.78'),
            'bolivia (plurinational state of)': ('-16.29', '-63.59'),
            'bolivia': ('-16.29', '-63.59'),
            'equatorial guinea': ('1.65', '10.27'),
            'syrian arab republic': ('34.80', '38.99'),
            'syria': ('34.80', '38.99'),  # Duplicate for safety
            'cabo verde': ('16.54', '-24.01'),
            'cape verde': ('16.54', '-24.01'),
            'viet nam': ('14.06', '108.28'),
            'vietnam': ('14.06', '108.28'),
            "lao people's democratic republic (the)": ('19.86', '102.50'),
            'laos': ('19.86', '102.50'),
            'congo': ('-4.04', '15.27'),
            'republic of the congo': ('-4.04', '15.27'),
            'guinea': ('9.95', '-9.70'),

            # Extended country mappings for complete coverage
            'burundi': ('-3.37', '29.92'),
            'cameroon': ('7.37', '12.35'),
            'botswana': ('-22.33', '24.68'),
            'cuba': ('21.52', '-77.78'),
            'comoros': ('-11.88', '43.87'),
            'costa rica': ('9.75', '-83.75'),
            'dominican republic': ('18.74', '-70.16'),
            'gabon': ('-0.80', '11.61'),
            'georgia': ('42.32', '43.36'),
            'honduras': ('15.20', '-86.24'),
            'kyrgyzstan': ('41.20', '74.77'),
            'libya': ('26.34', '17.23'),
            'malaysia': ('4.21', '101.98'),
            'mali': ('17.57', '-3.99'),
            'senegal': ('14.50', '-14.45'),
            'sri lanka': ('7.87', '80.77'),
            'thailand': ('15.87', '100.99'),
            'vanuatu': ('-15.38', '166.96'),
            'tajikistan': ('38.86', '71.28'),

            # Full name variations for better matching
            'bahamas': ('25.03', '-77.40'),
            'belize': ('17.19', '-88.50'),
            'central african republic': ('6.61', '20.94'),
            "côte d'ivoire": ('7.54', '-5.55'),
            'guinea-bissau': ('11.80', '-15.18'),
            'papua new guinea': ('-6.31', '143.96'),
            'united republic of tanzania': ('-6.37', '34.89'),
            'tanzania': ('-6.37', '34.89'),
            'venezuela (bolivarian republic of)': ('6.42', '-66.59'),
            'china - taiwan province': ('23.82', '121.56'),
            'taiwan': ('23.82', '121.56'),
            'bosnia and herzegovina': ('43.92', '17.68'),
            'american samoa': ('-14.31', '-170.70'),

            # Additional missing countries
            'cambodia': ('12.57', '104.99'),
            'panama': ('8.54', '-80.78')
        }

        country_lower = country_name.lower().strip()
        return country_coords.get(country_lower, ("", ""))

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