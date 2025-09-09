import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin
import sys
import os
import pathlib
from typing import Dict, Any, List

# =============================
# Helpers
# =============================

def _parse_iso(dt: str) -> float:
    """ISO 날짜 문자열을 timestamp로 변환"""
    try:
        if dt.endswith('Z'):
            dt = dt.replace('Z', '+00:00')
        elif not dt.endswith(('+00:00', '-')):
            dt += '+00:00'
        return datetime.fromisoformat(dt).timestamp()
    except Exception as e:
        print(f"Date parsing error for '{dt}': {e}")
        return 0.0

def clean_duplicate_key(title: str, date: str, lat: str, lon: str) -> str:
    """중복 제거용 키 생성 (정규화) - 더 엄격한 클러스터링 방지"""
    clean_title = re.sub(r'[^\w\s]', '', title.lower()).strip()
    clean_title = re.sub(r'\s+', ' ', clean_title)
    
    # Remove common words that don't help distinguish events
    common_words = ['earthquake', 'fire', 'flood', 'explosion', 'war', 'pollution', 'landslide', 'volcanic', 'eruption']
    title_words = [word for word in clean_title.split() if word not in common_words]
    clean_title = ' '.join(title_words[:5])  # Only keep first 5 significant words
    
    try:
        # 더 정밀한 좌표로 클러스터링 방지 (소수점 4자리로 충분, 너무 정밀하면 같은 사건도 다르게 인식)
        lat_clean = f"{float(lat):.4f}" if lat else "0"
        lon_clean = f"{float(lon):.4f}" if lon else "0"
        
        # 비슷한 위치 (0.01도 약 1km 내) 이벤트들은 같은 지역으로 간주
        lat_rounded = f"{round(float(lat) * 100) / 100:.2f}" if lat else "0"  
        lon_rounded = f"{round(float(lon) * 100) / 100:.2f}" if lon else "0"
        
        # Use rounded coordinates for clustering prevention, exact for exact duplicates
        location_key = f"{lat_rounded}|{lon_rounded}"
    except:
        lat_clean = "0"
        lon_clean = "0"
        location_key = "0|0"
    
    date_clean = date[:10] if len(date) >= 10 else date
    
    # Create a key that groups similar events in same area on same day
    return f"{clean_title}|{date_clean}|{location_key}"

def _stable_dedupe(urls: List[str]) -> List[str]:
    """순서 보존 중복 제거"""
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _event_id_from_url(u: str) -> int:
    m = re.search(r'/details/(\d+)', u)
    return int(m.group(1)) if m else -1

# =============================
# Merge & Validation
# =============================

def merge_events(new_events: List[Dict[str, Any]], 
                 existing_path: str = "docs/data/events.json",
                 past_events_path: str = "docs/data/past_events.json") -> List[Dict[str, Any]]:
    print("=" * 80)
    print("🔄 STARTING ENHANCED DATA MERGE PROCESS")
    print("=" * 80)

    merged: Dict[str, Dict[str, Any]] = {}
    stats = {
        'past_events_loaded': 0,
        'existing_events_loaded': 0,
        'new_events_provided': len(new_events),
        'new_events_added': 0,
        'events_updated': 0,
        'duplicates_removed': 0,
        'old_events_archived': 0,
        'validation_errors': 0
    }

    existing_path_obj = pathlib.Path(existing_path)
    past_path_obj = pathlib.Path(past_events_path)
    existing_path_obj.parent.mkdir(parents=True, exist_ok=True)
    past_path_obj.parent.mkdir(parents=True, exist_ok=True)

    print(f"📁 Working directories:\n  - Current events: {existing_path}\n  - Past events: {past_events_path}\n  - New events provided: {len(new_events)}")

    try:
        # 1) Load archived events (keep as is)
        print(f"\n📚 Loading archived events from {past_events_path}...")
        if past_path_obj.exists() and past_path_obj.stat().st_size > 0:
            try:
                past_content = past_path_obj.read_text(encoding="utf-8").strip()
                if past_content:
                    past_events = json.loads(past_content)
                    print(f"✅ Successfully loaded {len(past_events)} archived events")
                    for ev in past_events:
                        event_id = str(ev.get("event_id", "")).strip()
                        if event_id:
                            merged[event_id] = ev
                            stats['past_events_loaded'] += 1
                            merged[event_id]['_archived'] = True
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error in past events: {e}")
                backup_path = past_path_obj.with_suffix('.json.backup')
                past_path_obj.rename(backup_path)
                print(f"📦 Corrupted file backed up to {backup_path}")

        # 2) Load current events
        print(f"\n📂 Loading current events from {existing_path}...")
        if existing_path_obj.exists() and existing_path_obj.stat().st_size > 0:
            try:
                existing_content = existing_path_obj.read_text(encoding="utf-8").strip()
                if existing_content:
                    existing_events = json.loads(existing_content)
                    print(f"✅ Successfully loaded {len(existing_events)} current events")
                    for ev in existing_events:
                        event_id = str(ev.get("event_id", "")).strip()
                        if not event_id:
                            continue
                        existing_in_merged = merged.get(event_id)
                        if existing_in_merged and existing_in_merged.get('_archived'):
                            continue
                        elif existing_in_merged:
                            existing_time = _parse_iso(ev.get("crawled_at", ""))
                            merged_time = _parse_iso(existing_in_merged.get("crawled_at", ""))
                            if existing_time > merged_time:
                                merged[event_id] = ev
                        else:
                            merged[event_id] = ev
                            stats['existing_events_loaded'] += 1
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error in current events: {e}")
                stats['validation_errors'] += 1

        # 3) Integrate new events
        print(f"\n🆕 Processing {len(new_events)} new events...")
        for i, ev in enumerate(new_events, 1):
            if i % 50 == 0:
                print(f"  ⏳ Processing event {i}/{len(new_events)}...")
            if not isinstance(ev, dict):
                stats['validation_errors'] += 1
                continue
            event_id = str(ev.get("event_id", "")).strip()
            if not event_id:
                title = str(ev.get('event_title', '')).strip()[:50]
                date = str(ev.get('event_date_utc', '')).strip()[:10]
                lat = str(ev.get('latitude', '')).strip()[:10]
                lon = str(ev.get('longitude', '')).strip()[:10]
                if not title:
                    stats['validation_errors'] += 1
                    continue
                event_id = f"TEMP_{hash(f'{title}_{date}_{lat}_{lon}')}"
                ev["event_id"] = event_id
            existing = merged.get(event_id)
            if existing and existing.get('_archived'):
                continue
            elif existing:
                new_time = _parse_iso(ev.get("crawled_at", ""))
                existing_time = _parse_iso(existing.get("crawled_at", ""))
                if new_time >= existing_time:
                    merged[event_id] = ev
                    stats['events_updated'] += 1
            else:
                merged[event_id] = ev
                stats['new_events_added'] += 1
        print(f"✅ ID-based merge completed: {len(merged)} unique events")

        # 4) Content-based dedupe (keep order by archived first)
        print(f"\n🔍 Performing content-based deduplication...")
        seen_keys = set()
        deduped = []
        duplicate_count = 0
        archived_events = [ev for ev in merged.values() if ev.get('_archived')]
        non_archived_events = [ev for ev in merged.values() if not ev.get('_archived')]
        for ev in archived_events + non_archived_events:
            title = str(ev.get('event_title', '')).strip()
            if not title:
                stats['validation_errors'] += 1
                continue
            date = str(ev.get('event_date_utc', '')).strip()
            lat = str(ev.get('latitude', '')).strip()
            lon = str(ev.get('longitude', '')).strip()
            key = clean_duplicate_key(title, date, lat, lon)
            if key not in seen_keys:
                seen_keys.add(key)
                if '_archived' in ev:
                    ev = {**ev}
                    ev.pop('_archived', None)
                deduped.append(ev)
            else:
                duplicate_count += 1
        stats['duplicates_removed'] = duplicate_count
        print(f"✅ Content-based deduplication: removed {duplicate_count} duplicates")

        # 5) Split by age and archive old items (>30 days)
        cutoff_date = datetime.now() - timedelta(days=30)
        recent_events, old_events = [], []
        print(f"\n📅 Separating events by age (cutoff: {cutoff_date.strftime('%Y-%m-%d')})...")
        for event in deduped:
            try:
                crawl_time_str = event.get('crawled_at', '')
                if crawl_time_str:
                    crawl_time = datetime.fromisoformat(crawl_time_str.replace('Z', '+00:00'))
                    (recent_events if crawl_time >= cutoff_date else old_events).append(event)
                else:
                    recent_events.append(event)
            except Exception:
                recent_events.append(event)
        stats['old_events_archived'] = len(old_events)
        print(f"  📊 Recent events (keep in main): {len(recent_events)}")
        print(f"  📚 Old events (move to archive): {len(old_events)}")

        # 6) Update archive file if needed
        if old_events:
            try:
                existing_past = []
                if past_path_obj.exists():
                    past_content = past_path_obj.read_text(encoding="utf-8").strip()
                    if past_content:
                        existing_past = json.loads(past_content)
                archive_merged = {}
                for ev in existing_past + old_events:
                    event_id = ev.get("event_id")
                    if not event_id:
                        continue
                    prev = archive_merged.get(event_id)
                    if prev is None or _parse_iso(ev.get("crawled_at", "")) > _parse_iso(prev.get("crawled_at", "")):
                        archive_merged[event_id] = ev
                final_archive = list(archive_merged.values())
                final_archive.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
                with open(past_path_obj, 'w', encoding='utf-8') as f:
                    json.dump(final_archive, f, ensure_ascii=False, indent=2)
                print(f"✅ Updated archive with {len(final_archive)} total archived events")
            except Exception as e:
                print(f"❌ Error updating archive: {e}")
                stats['validation_errors'] += 1

        # 7) Final sort & validate
        final_events = recent_events
        final_events.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
        valid_events = []
        for event in final_events:
            if event.get('event_title') and event.get('event_id') and event.get('event_category'):
                valid_events.append(event)
            else:
                stats['validation_errors'] += 1
        final_events = valid_events

        print("\n" + "=" * 60)
        print("📊 MERGE PROCESS COMPLETED")
        print("=" * 60)
        print(f"Past events loaded: {stats['past_events_loaded']}")
        print(f"Existing events loaded: {stats['existing_events_loaded']}")
        print(f"New events provided: {stats['new_events_provided']}")
        print(f"New events added: {stats['new_events_added']}")
        print(f"Events updated: {stats['events_updated']}")
        print(f"Content duplicates removed: {stats['duplicates_removed']}")
        print(f"Events archived: {stats['old_events_archived']}")
        print(f"Validation errors: {stats['validation_errors']}")
        print(f"Final events in main file: {len(final_events)}")
        if final_events:
            category_stats = {}
            for event in final_events:
                cat = event.get('event_category', 'Unknown')
                category_stats[cat] = category_stats.get(cat, 0) + 1
            print(f"\n📈 Categories in final dataset:")
            for cat, count in sorted(category_stats.items()):
                print(f"  {cat}: {count}")
        print("=" * 60)
        return final_events

    except Exception as e:
        print(f"\n💥 CRITICAL ERROR in merge process: {e}")
        import traceback
        traceback.print_exc()
        print(f"🔄 Emergency fallback: returning new events only")
        return new_events if new_events else []


def create_backup_if_needed(events_path: str = "docs/data/events.json"):
    try:
        events_file = pathlib.Path(events_path)
        if not events_file.exists() or events_file.stat().st_size == 0:
            print("⚠️ No data to backup")
            return
        backup_dir = pathlib.Path("docs/data/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"events_backup_{timestamp}.json"
        backup_path.write_text(events_file.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"✅ Created timestamped backup: {backup_path}")
        run_number = os.environ.get('GITHUB_RUN_NUMBER')
        if run_number:
            run_backup_path = backup_dir / f"events_run_{run_number}.json"
            try:
                backup_path.link_to(run_backup_path.resolve())
            except Exception:
                pass
            print(f"✅ Created run-based backup: {run_backup_path}")
        # Smart backup cleanup - keep only recent and important backups
        backups = sorted(backup_dir.glob("events_backup_*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        run_backups = sorted(backup_dir.glob("events_run_*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        
        # Keep only 5 most recent timestamp backups
        if len(backups) > 5:
            for old_backup in backups[5:]:
                old_backup.unlink()
                print(f"🗑️ Removed old timestamp backup: {old_backup.name}")
        
        # Keep only 10 most recent run backups, but prioritize larger files
        if len(run_backups) > 10:
            # Sort by size (larger files first) among old backups
            old_run_backups = run_backups[10:]
            old_run_backups.sort(key=lambda x: x.stat().st_size, reverse=True)
            # Keep 2 largest old backups, remove the rest
            for old_backup in old_run_backups[2:]:
                old_backup.unlink()
                print(f"🗑️ Removed old run backup: {old_backup.name}")
    except Exception as e:
        print(f"⚠️ Error creating backup: {e}")


def update_past_events_archive():
    try:
        events_path = pathlib.Path("docs/data/events.json")
        past_path = pathlib.Path("docs/data/past_events.json")
        if not events_path.exists():
            return
        with open(events_path, 'r', encoding='utf-8') as f:
            current_events = json.load(f)
        past_events = []
        if past_path.exists():
            try:
                with open(past_path, 'r', encoding='utf-8') as f:
                    past_events = json.load(f)
            except:
                pass
        cutoff_date = datetime.now() - timedelta(days=30)
        recent_events, old_events = [], []
        for event in current_events:
            crawled_at = event.get('crawled_at', '')
            try:
                if crawled_at:
                    crawl_time = datetime.fromisoformat(crawled_at.replace('Z', '+00:00'))
                    (recent_events if crawl_time >= cutoff_date else old_events).append(event)
                else:
                    recent_events.append(event)
            except:
                recent_events.append(event)
        if old_events:
            print(f"✓ Archiving {len(old_events)} old events to past_events.json")
            all_past_events = past_events + old_events
            past_merged = {}
            for ev in all_past_events:
                event_id = str(ev.get("event_id", "")).strip()
                if not event_id:
                    continue
                prev = past_merged.get(event_id)
                if prev is None or _parse_iso(ev.get("crawled_at", "")) > _parse_iso(prev.get("crawled_at", "")):
                    past_merged[event_id] = ev
            final_past_events = list(past_merged.values())
            final_past_events.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
            with open(past_path, 'w', encoding='utf-8') as f:
                json.dump(final_past_events, f, ensure_ascii=False, indent=2)
            with open(events_path, 'w', encoding='utf-8') as f:
                json.dump(recent_events, f, ensure_ascii=False, indent=2)
            print(f"✓ Updated events.json with {len(recent_events)} recent events")
            print(f"✓ Updated past_events.json with {len(final_past_events)} archived events")
    except Exception as e:
        print(f"⚠️ Error updating past events archive: {e}")

# =============================
# Crawler
# =============================

class RSOECrawler:
    def __init__(self):
        self.base_url = "https://rsoe-edis.org"
        self.event_list_url = "https://rsoe-edis.org/eventList"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.target_categories = {
            "War": "War",
            "Environment pollution": "Environment pollution",
            "Industrial explosion": "Industrial explosion",
            "Surroundings explosion": "Surroundings explosion",
            "Fire in built environment": "Fire in built environment",
            "Earthquake": "Earthquake",
            "Landslide": "Landslide",
            "Volcanic eruption": "Volcanic eruption",
            "Flood": "Flood"
        }
        self.collected_events = []
        self.existing_events = set()  # Store existing event IDs and content hashes
        self.existing_content_keys = set()  # Store content-based duplicate keys
        self.load_existing_events()  # Load existing events on initialization

    def load_existing_events(self):
        """기존 이벤트들을 로드해서 중복 체크용 데이터 구축"""
        try:
            # Load current events
            current_path = pathlib.Path("docs/data/events.json")
            if current_path.exists():
                with open(current_path, 'r', encoding='utf-8') as f:
                    current_events = json.load(f)
                for event in current_events:
                    event_id = str(event.get("event_id", "")).strip()
                    if event_id:
                        self.existing_events.add(event_id)
                    # Create content key for duplicate detection
                    title = str(event.get('event_title', '')).strip()
                    date = str(event.get('event_date_utc', '')).strip()
                    lat = str(event.get('latitude', '')).strip()
                    lon = str(event.get('longitude', '')).strip()
                    if title:
                        content_key = clean_duplicate_key(title, date, lat, lon)
                        self.existing_content_keys.add(content_key)
            
            # Load past events too
            past_path = pathlib.Path("docs/data/past_events.json")
            if past_path.exists():
                with open(past_path, 'r', encoding='utf-8') as f:
                    past_events = json.load(f)
                for event in past_events:
                    event_id = str(event.get("event_id", "")).strip()
                    if event_id:
                        self.existing_events.add(event_id)
                    # Create content key for duplicate detection
                    title = str(event.get('event_title', '')).strip()
                    date = str(event.get('event_date_utc', '')).strip()
                    lat = str(event.get('latitude', '')).strip()
                    lon = str(event.get('longitude', '')).strip()
                    if title:
                        content_key = clean_duplicate_key(title, date, lat, lon)
                        self.existing_content_keys.add(content_key)
            
            print(f"✓ Loaded {len(self.existing_events)} existing event IDs and {len(self.existing_content_keys)} content keys for duplicate detection")
        except Exception as e:
            print(f"⚠️ Error loading existing events: {e}")

    def get_page_content(self, url, retries=2):
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=15)  # Reduced timeout
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                print(f"⚠️ Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(1)  # Reduced retry delay
                else:
                    print(f"✗ Failed to fetch {url} after {retries} attempts")
                    return None

    def extract_all_event_links(self, html_content):
        if not html_content:
            return []
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = []
        links = soup.find_all('a', href=re.compile(r'/eventList/details/\d+'))
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                event_links.append(full_url)
        print(f"✓ Found {len(event_links)} event links on this page")
        # IMPORTANT: stable dedupe (do NOT use set)
        return _stable_dedupe(event_links)

    def find_pagination_links(self, html_content):
        if not html_content:
            return []
        soup = BeautifulSoup(html_content, 'html.parser')
        page_urls = []
        pagination_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        for link in pagination_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                page_urls.append(full_url)
        # IMPORTANT: stable dedupe (do NOT use set)
        return _stable_dedupe(page_urls)

    def _map_category(self, raw_category: str) -> str:
        # Enhanced category mapping with better patterns
        raw = raw_category.strip()
        norm = raw.lower()
        norm = re.sub(r'\bthe\b', ' ', norm)
        norm = re.sub(r'\s+', ' ', norm).strip()
        
        # More comprehensive mapping patterns - ONLY target categories
        aliases = [
            # War-related - more patterns
            (r'\b(war|conflict|armed conflict|military|warfare|battle)\b', 'War'),
            
            # Environment pollution - more patterns
            (r'\b(environment(al)? pollution|pollution|chemical spill|toxic|contamination|hazardous|oil spill)\b', 'Environment pollution'),
            
            # Explosions - more specific patterns
            (r'\b(industrial explosion|factory explosion|plant explosion|refinery explosion)\b', 'Industrial explosion'),
            (r'\b(surroundings? explosion|explosion|blast|detonation)\b', 'Surroundings explosion'),
            
            # Fires - more specific patterns for built environment only
            (r'\b(fire in (the )?built environment|building fire|house fire|structure fire|residential fire|apartment fire|urban fire)\b', 'Fire in built environment'),
            
            # Natural disasters - more patterns
            (r'\b(earthquake|quake|seismic|tremor|aftershock)\b', 'Earthquake'),
            (r'\b(landslide|mudslide|rockslide|slope failure|debris flow)\b', 'Landslide'),
            (r'\b(volcan(ic|o) eruption|volcanic activity|volcano|lava|ash cloud)\b', 'Volcanic eruption'),
            (r'\b(flash ?flood|floods?|flooding|inundation|deluge)\b', 'Flood'),
        ]
        
        # Try pattern matching first
        for patt, mapped in aliases:
            if re.search(patt, norm):
                return mapped
                
        # Fallback: partial matching with target categories
        for target_cat, mapped_cat in self.target_categories.items():
            if target_cat.lower() in norm or any(word in norm for word in target_cat.lower().split()):
                return mapped_cat
                
        # Return empty string for unmapped categories (will be filtered out)
        return ''

    def extract_event_details(self, event_url):
        try:
            html_content = self.get_page_content(event_url)
            if not html_content:
                return None
            soup = BeautifulSoup(html_content, 'html.parser')
            event_id_match = re.search(r'/details/(\d+)', event_url)
            event_id = event_id_match.group(1) if event_id_match else ""
            fields = {}
            dt_elements = soup.find_all('dt')
            for dt in dt_elements:
                field_name = dt.get_text(strip=True)
                dd = dt.find_next_sibling('dd')
                if dd:
                    field_value = dd.get_text(strip=True)
                    fields[field_name] = field_value
            if not fields:
                rows = soup.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        field_name = cells[0].get_text(strip=True)
                        field_value = cells[1].get_text(strip=True)
                        if field_name and field_value:
                            fields[field_name] = field_value
            if not fields.get("Event title") or not fields.get("Event category"):
                all_text = soup.get_text()
                lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                for i, line in enumerate(lines):
                    if 'Event title' in line and i + 1 < len(lines):
                        fields["Event title"] = lines[i + 1]
                    elif 'Event category' in line and i + 1 < len(lines):
                        fields["Event category"] = lines[i + 1]
                    elif 'Event date (UTC)' in line and i + 1 < len(lines):
                        fields["Event date (UTC)"] = lines[i + 1]
                    elif 'Last update (UTC)' in line and i + 1 < len(lines):
                        fields["Last update (UTC)"] = lines[i + 1]
                    elif 'Latitude' in line and i + 1 < len(lines):
                        fields["Latitude"] = lines[i + 1]
                    elif 'Longitude' in line and i + 1 < len(lines):
                        fields["Longitude"] = lines[i + 1]
                    elif 'Area range' in line and i + 1 < len(lines):
                        fields["Area range"] = lines[i + 1]
                    elif 'Address/Affected area(s)' in line and i + 1 < len(lines):
                        fields["Address/Affected area(s)"] = lines[i + 1]
            title = fields.get("Event title", "").strip()
            category_raw = fields.get("Event category", "").strip()
            mapped_category = self._map_category(category_raw)
            if not mapped_category:
                return None
            # Source link
            source_link = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.startswith('http') and 'rsoe-edis.org' not in href:
                    link_text = link.get_text(strip=True).lower()
                    parent_text = link.parent.get_text().lower() if link.parent else ''
                    if 'source' in link_text or 'source' in parent_text:
                        source_link = href
                        break
            if not source_link:
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('http') and 'rsoe-edis.org' not in href:
                        source_link = href
                        break
            # Coordinates
            latitude = fields.get("Latitude", "").strip()
            longitude = fields.get("Longitude", "").strip()
            try:
                if latitude:
                    lat_clean = re.sub(r'[°NSEW]', '', latitude).strip()
                    lat_float = float(lat_clean)
                    latitude = str(lat_float) if (-90 <= lat_float <= 90) else ""
                if longitude:
                    lon_clean = re.sub(r'[°NSEW]', '', longitude).strip()
                    lon_float = float(lon_clean)
                    longitude = str(lon_float) if (-180 <= lon_float <= 180) else ""
            except ValueError:
                latitude = ""
                longitude = ""
            return {
                "event_id": event_id,
                "event_title": title,
                "event_category": mapped_category,
                "original_category": category_raw,
                "source": source_link or "",
                "event_date_utc": fields.get("Event date (UTC)", "").strip(),
                "last_update_utc": fields.get("Last update (UTC)", "").strip(),
                "latitude": latitude,
                "longitude": longitude,
                "area_range": fields.get("Area range", "").strip(),
                "address": fields.get("Address/Affected area(s)", "").strip(),
                "crawled_at": datetime.now().isoformat(),
                "event_url": event_url,
            }
        except Exception as e:
            print(f"⚠️ Error extracting details from {event_url}: {e}")
            return None

    def crawl_events(self):
        print("=== STARTING RSOE EDIS EVENT CRAWLING ===")
        print(f"Target categories: {list(self.target_categories.keys())}")
        print("=" * 60)
        try:
            print("✓ Loading main event list page...")
            main_html = self.get_page_content(self.event_list_url)
            if not main_html:
                print("✗ Failed to load main page")
                return False
            all_event_links = self.extract_all_event_links(main_html)
            print("✓ Looking for additional pages...")
            pagination_links = self.find_pagination_links(main_html)
            if pagination_links:
                print(f"✓ Found {len(pagination_links)} pagination links")
                max_pages = min(len(pagination_links), 3)  # Only check 3 additional pages
                for i, page_url in enumerate(pagination_links[:max_pages]):
                    try:
                        print(f"  → Loading additional page {i+1}/{max_pages}...")
                        page_html = self.get_page_content(page_url)
                        if page_html:
                            page_links = self.extract_all_event_links(page_html)
                            all_event_links.extend(page_links)
                        time.sleep(0.5)  # Reduced page delay
                    except Exception as e:
                        print(f"⚠️ Error loading page {page_url}: {e}")
            # IMPORTANT: keep order & dedupe, then newest-first by /details/<id>
            all_event_links = _stable_dedupe(all_event_links)
            all_event_links.sort(key=_event_id_from_url, reverse=True)
            print(f"✓ Total unique event links collected: {len(all_event_links)}")
            if not all_event_links:
                print("✗ No event links found!")
                return False
            print("=" * 60)
            print("✓ Processing event detail pages...")
            print("=" * 60)
            target_events_found = 0
            consecutive_duplicates = 0  # Track consecutive duplicates for early termination
            max_consecutive_duplicates = 20  # Stop if we hit this many duplicates in a row
            # Process more events since we'll terminate early on duplicates
            max_events_to_process = min(len(all_event_links), 500)
            
            for i, event_url in enumerate(all_event_links[:max_events_to_process], 1):
                if i % 50 == 0:
                    print(f"[{i:3d}/{max_events_to_process}] Progress: {i/max_events_to_process*100:.1f}%")
                event_data = self.extract_event_details(event_url)
                if event_data and event_data['event_category'] in self.target_categories.values():
                    event_id = event_data['event_id']
                    category = event_data['event_category']
                    title = event_data['event_title']
                    
                    # Create content key for duplicate detection
                    date = event_data['event_date_utc']
                    lat = event_data['latitude']
                    lon = event_data['longitude']
                    content_key = clean_duplicate_key(title, date, lat, lon)
                    
                    # Check for duplicates (both ID and content-based)
                    is_duplicate = (
                        event_id in self.existing_events or 
                        content_key in self.existing_content_keys
                    )
                    
                    if is_duplicate:
                        consecutive_duplicates += 1
                        if i <= 20:
                            print(f"  🔄 [{i:3d}] DUPLICATE: {event_id} - {category} - {title[:30]}...")
                    else:
                        consecutive_duplicates = 0  # Reset counter on new event
                        self.collected_events.append(event_data)
                        self.existing_events.add(event_id)  # Add to prevent future duplicates in this run
                        self.existing_content_keys.add(content_key)
                        target_events_found += 1
                        if i <= 10:
                            print(f"  ✓ [{i:3d}] COLLECTED: {event_id} - {category} - {title[:40]}...")
                    
                elif event_data and event_data['event_category'] not in self.target_categories.values():
                    if i <= 20:  # Show first 20 skipped categories for debugging
                        print(f"  ⏭️ [{i:3d}] SKIPPED: {event_data['event_id']} - '{event_data['original_category']}' -> '{event_data['event_category']}'")
                
                # Early termination if too many consecutive duplicates
                if consecutive_duplicates >= max_consecutive_duplicates:
                    print(f"\\n⏹️ Early termination: {consecutive_duplicates} consecutive duplicates found after processing {i} events")
                    print(f"   This suggests we've caught up with existing data.")
                    break
                    
                # Minimal sleep to avoid being rate-limited
                time.sleep(0.03)
            print("\n" + "=" * 60)
            print("✓ CRAWLING COMPLETED!")
            print(f"Total events processed: {i} of {max_events_to_process}")
            print(f"New events collected: {len(self.collected_events)}")
            print(f"Consecutive duplicates at end: {consecutive_duplicates}")
            if consecutive_duplicates >= max_consecutive_duplicates:
                print(f"🎯 Stopped early due to {consecutive_duplicates} consecutive duplicates - likely caught up with existing data!")
            if self.collected_events:
                category_stats = {}
                for event in self.collected_events:
                    cat = event['event_category']
                    category_stats[cat] = category_stats.get(cat, 0) + 1
                print("\nNew events by category:")
                for cat, count in sorted(category_stats.items()):
                    print(f"  {cat}: {count}")
            print("=" * 60)
            return True
        except Exception as e:
            print(f"✗ Error during crawling: {e}")
            import traceback
            traceback.print_exc()
            return False

    def save_to_json(self, filename="docs/data/events.json"):
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.collected_events, f, ensure_ascii=False, indent=2)
            print(f"✓ Data saved to {filename}")
            return True
        except Exception as e:
            print(f"✗ Error saving to JSON: {e}")
            return False

# =============================
# Entrypoint
# =============================

def main():
    print("=" * 80)
    print("🌍 RSOE DISASTER DATA CRAWLER WITH CUMULATIVE MERGE")
    print("=" * 80)
    try:
        print("1. Creating backup of existing data...")
        create_backup_if_needed("docs/data/events.json")
        print("\n2. Initializing crawler...")
        crawler = RSOECrawler()
        print("\n3. Starting crawling process...")
        success = crawler.crawl_events()
        if success and crawler.collected_events:
            print(f"\n4. Successfully collected {len(crawler.collected_events)} new events")
            category_counts = {}
            for event in crawler.collected_events:
                category = event['event_category']
                category_counts[category] = category_counts.get(category, 0) + 1
            print("\n=== NEW COLLECTION SUMMARY ===")
            print(f"Total new events: {len(crawler.collected_events)}")
            print("New events by category:")
            for category, count in sorted(category_counts.items()):
                print(f"  📊 {category}: {count}")
        else:
            print(f"\n4. No new events collected (success={success})")
            crawler.collected_events = []
        print(f"\n5. Merging with existing data...")
        merged = merge_events(
            crawler.collected_events,
            existing_path="docs/data/events.json",
            past_events_path="docs/data/past_events.json",
        )
        print(f"\n6. Saving merged results...")
        output_file = "docs/data/events.json"  # IMPORTANT: site reads from here
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        print(f"✓ Final merged events saved to {output_file}: {len(merged)} total events")
        print(f"\n7. Updating past events archive...")
        update_past_events_archive()
        if merged:
            final_categories = {}
            latest_events = 0
            cutoff = datetime.now() - timedelta(days=7)
            for event in merged:
                cat = event.get('event_category', 'Unknown')
                final_categories[cat] = final_categories.get(cat, 0) + 1
                crawl_time = event.get('crawled_at', '')
                if crawl_time:
                    try:
                        crawl_dt = datetime.fromisoformat(crawl_time.replace('Z', '+00:00'))
                        if crawl_dt >= cutoff:
                            latest_events += 1
                    except:
                        pass
            print("\n=== FINAL DATABASE SUMMARY ===")
            print(f"📈 Total events in database: {len(merged)}")
            print(f"🆕 Events from last 7 days: {latest_events}")
            print("📊 Categories in database:")
            for category, count in sorted(final_categories.items()):
                print(f"  {category}: {count}")
        print("\n✅ PROCESS COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        return 0
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 80)
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
