import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import sys
import os

# 파일 상단 import들 아래에 추가
import pathlib
from typing import Dict, Any, List

def _parse_iso(dt: str) -> float:
    """ISO 날짜 문자열을 timestamp로 변환"""
    try:
        # Z 또는 +00:00 형태 처리
        if dt.endswith('Z'):
            dt = dt.replace('Z', '+00:00')
        elif not dt.endswith(('+00:00', '-')):
            dt += '+00:00'
        return datetime.fromisoformat(dt).timestamp()
    except Exception as e:
        print(f"Date parsing error for '{dt}': {e}")
        return 0.0

def clean_duplicate_key(title: str, date: str, lat: str, lon: str) -> str:
    """중복 제거용 키 생성 (정규화)"""
    # 제목 정규화: 소문자, 특수문자 제거, 공백 정규화
    clean_title = re.sub(r'[^\w\s]', '', title.lower()).strip()
    clean_title = re.sub(r'\s+', ' ', clean_title)
    
    # 좌표 정규화: 소수점 4자리로 반올림
    try:
        lat_clean = f"{float(lat):.4f}" if lat else "0"
        lon_clean = f"{float(lon):.4f}" if lon else "0"
    except:
        lat_clean = "0"
        lon_clean = "0"
    
    # 날짜 정규화: 일자만 추출
    date_clean = date[:10] if len(date) >= 10 else date
    
    return f"{clean_title}|{date_clean}|{lat_clean}|{lon_clean}"

def merge_events(new_events: List[Dict[str, Any]], 
                 existing_path: str = "docs/data/events.json",
                 past_events_path: str = "docs/data/past_events.json") -> List[Dict[str, Any]]:
    """
    새로운 이벤트와 기존 이벤트를 병합하여 누적 저장
    - event_id 기준으로 중복 제거 (1차)
    - 같은 event_id면 최신 crawled_at 우선
    - 보조 키(title, date, lat, lon)로 한 번 더 중복 제거 (2차)
    - past_events.json도 함께 고려
    - 오래된 이벤트는 자동으로 정리 (선택적)
    """
    print("=== STARTING DATA MERGE PROCESS ===")
    
    merged: Dict[str, Dict[str, Any]] = {}
    stats = {
        'past_events_loaded': 0,
        'existing_events_loaded': 0,
        'new_events_provided': len(new_events),
        'new_events_added': 0,
        'events_updated': 0,
        'duplicates_removed': 0,
        'old_events_removed': 0
    }

    # 1) past_events.json 로드 (최고 우선순위)
    past_path = pathlib.Path(past_events_path)
    if past_path.exists():
        try:
            past_content = past_path.read_text(encoding="utf-8")
            if past_content.strip():
                past_events = json.loads(past_content)
                print(f"✓ Loaded {len(past_events)} past events from {past_events_path}")
                for ev in past_events:
                    event_id = str(ev.get("event_id", "")).strip()
                    if event_id and event_id != "":
                        merged[event_id] = ev
                        stats['past_events_loaded'] += 1
            else:
                print(f"⚠️ {past_events_path} is empty")
        except Exception as e:
            print(f"⚠️ Error loading past events: {e}")

    # 2) 기존 events.json 로드
    existing_path_obj = pathlib.Path(existing_path)
    if existing_path_obj.exists():
        try:
            existing_content = existing_path_obj.read_text(encoding="utf-8")
            if existing_content.strip():
                existing_events = json.loads(existing_content)
                print(f"✓ Loaded {len(existing_events)} existing events from {existing_path}")
                for ev in existing_events:
                    event_id = str(ev.get("event_id", "")).strip()
                    if event_id and event_id != "":
                        # 기존 것이 더 최신이면 유지, 아니면 갱신
                        if event_id in merged:
                            existing_time = _parse_iso(ev.get("crawled_at", ""))
                            merged_time = _parse_iso(merged[event_id].get("crawled_at", ""))
                            if existing_time > merged_time:
                                merged[event_id] = ev
                                print(f"  → Updated event {event_id} with newer data from existing")
                        else:
                            merged[event_id] = ev
                            stats['existing_events_loaded'] += 1
            else:
                print(f"⚠️ {existing_path} is empty")
        except Exception as e:
            print(f"⚠️ Error loading existing events: {e}")

    # 3) 새 데이터로 갱신 (같은 ID면 최신 crawled_at 우선)
    print(f"✓ Processing {len(new_events)} new events...")
    
    for i, ev in enumerate(new_events, 1):
        if i % 50 == 0:
            print(f"  → Processing event {i}/{len(new_events)}...")
            
        event_id = str(ev.get("event_id", "")).strip()
        
        # event_id가 없는 경우 임시 ID 생성
        if not event_id or event_id == "":
            title = str(ev.get('event_title', '')).strip()[:50]
            date = str(ev.get('event_date_utc', '')).strip()[:10]
            lat = str(ev.get('latitude', '')).strip()[:10]
            lon = str(ev.get('longitude', '')).strip()[:10]
            event_id = f"TEMP_{hash(f'{title}_{date}_{lat}_{lon}')}"
            ev["event_id"] = event_id
            
        prev = merged.get(event_id)
        if prev:
            # 기존 이벤트 있음 - 시간 비교해서 갱신
            new_time = _parse_iso(ev.get("crawled_at", ""))
            prev_time = _parse_iso(prev.get("crawled_at", ""))
            if new_time >= prev_time:
                merged[event_id] = ev
                stats['events_updated'] += 1
                if i <= 10:  # 처음 10개만 로그
                    print(f"  ✓ Updated event: {event_id}")
        else:
            # 새 이벤트
            merged[event_id] = ev
            stats['new_events_added'] += 1
            if i <= 10:  # 처음 10개만 로그
                print(f"  ✓ New event: {event_id}")

    print(f"✓ ID-based merge completed: {len(merged)} unique events by ID")

    # 4) 보조 키로도 한 번 더 중복 제거 (서로 다른 ID지만 사실상 동일한 경우)
    print("✓ Performing secondary deduplication by content similarity...")
    seen_keys = set()
    deduped = []
    
    for ev in merged.values():
        # 제목, 날짜, 좌표로 중복 판단
        title = str(ev.get('event_title', '')).strip()
        date = str(ev.get('event_date_utc', '')).strip()
        lat = str(ev.get('latitude', '')).strip()
        lon = str(ev.get('longitude', '')).strip()
        
        # 필수 데이터가 없는 경우 스킵
        if not title:
            continue
            
        key = clean_duplicate_key(title, date, lat, lon)
        
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(ev)
        else:
            stats['duplicates_removed'] += 1

    # 5) 오래된 이벤트 정리 (6개월 이상 된 것들 - 선택적)
    if len(deduped) > 1000:  # 이벤트가 많을 때만 정리
        print("✓ Cleaning up very old events...")
        cutoff_date = datetime.now() - timedelta(days=180)  # 6개월
        
        filtered = []
        for ev in deduped:
            event_date_str = ev.get('event_date_utc', '')
            try:
                if event_date_str:
                    event_date = datetime.fromisoformat(event_date_str.replace('Z', '+00:00'))
                    if event_date >= cutoff_date:
                        filtered.append(ev)
                    else:
                        stats['old_events_removed'] += 1
                else:
                    filtered.append(ev)  # 날짜 정보 없으면 보존
            except:
                filtered.append(ev)  # 날짜 파싱 실패하면 보존
        
        if stats['old_events_removed'] > 0:
            print(f"  → Removed {stats['old_events_removed']} events older than 6 months")
            deduped = filtered

    # 6) 최신순 정렬 (crawled_at 기준)
    deduped.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
    
    # 통계 출력
    print("\n=== MERGE STATISTICS ===")
    print(f"Past events loaded: {stats['past_events_loaded']}")
    print(f"Existing events loaded: {stats['existing_events_loaded']}")
    print(f"New events provided: {stats['new_events_provided']}")
    print(f"New events added: {stats['new_events_added']}")
    print(f"Events updated: {stats['events_updated']}")
    print(f"Duplicates removed: {stats['duplicates_removed']}")
    print(f"Old events removed: {stats['old_events_removed']}")
    print(f"Final total events: {len(deduped)}")
    print("==========================")
    
    return deduped

def create_backup_if_needed(events_path: str = "docs/data/events.json"):
    """
    현재 events.json을 백업으로 저장 (날짜별)
    """
    try:
        events_file = pathlib.Path(events_path)
        if events_file.exists() and events_file.stat().st_size > 0:
            backup_dir = pathlib.Path("docs/data/backups")
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            today = datetime.now().strftime("%Y%m%d_%H%M")
            backup_path = backup_dir / f"events_backup_{today}.json"
            
            # 같은 시간대 백업이 없을 때만 생성
            if not backup_path.exists():
                backup_path.write_text(events_file.read_text(encoding="utf-8"), encoding="utf-8")
                print(f"✓ Created backup: {backup_path}")
                
                # 오래된 백업 정리 (7일 이상)
                cutoff_time = datetime.now() - timedelta(days=7)
                for backup_file in backup_dir.glob("events_backup_*.json"):
                    if backup_file.stat().st_mtime < cutoff_time.timestamp():
                        backup_file.unlink()
                        print(f"  → Removed old backup: {backup_file.name}")
            else:
                print(f"⚠️ Backup already exists for this time: {backup_path}")
    except Exception as e:
        print(f"⚠️ Error creating backup: {e}")


class RSOECrawler:
    def __init__(self):
        self.base_url = "https://rsoe-edis.org"
        self.event_list_url = "https://rsoe-edis.org/eventList"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # 필터링할 카테고리들
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
        
    def get_page_content(self, url, retries=3):
        """페이지 내용을 가져오는 함수"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                print(f"⚠️ Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"✗ Failed to fetch {url} after {retries} attempts")
                    return None
    
    def extract_all_event_links(self, html_content):
        """이벤트 리스트에서 모든 이벤트 링크들 추출"""
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
        return list(set(event_links))
    
    def find_pagination_links(self, html_content):
        """페이지네이션 링크들 찾기"""
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
        
        return list(set(page_urls))
    
    def extract_event_details(self, event_url):
        """개별 이벤트의 상세 정보 추출"""
        try:
            html_content = self.get_page_content(event_url)
            if not html_content:
                return None
                
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 이벤트 ID 추출
            event_id_match = re.search(r'/details/(\d+)', event_url)
            event_id = event_id_match.group(1) if event_id_match else ""
            
            fields = {}
            
            # dt/dd 태그 쌍으로 정보 추출
            dt_elements = soup.find_all('dt')
            for dt in dt_elements:
                field_name = dt.get_text(strip=True)
                dd = dt.find_next_sibling('dd')
                if dd:
                    field_value = dd.get_text(strip=True)
                    fields[field_name] = field_value
            
            # 테이블 구조에서 정보 추출
            if not fields:
                rows = soup.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        field_name = cells[0].get_text(strip=True)
                        field_value = cells[1].get_text(strip=True)
                        if field_name and field_value:
                            fields[field_name] = field_value
            
            # 텍스트 패턴 매칭으로 추출
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
            category = fields.get("Event category", "").strip()
            
            # 카테고리 매핑 및 필터링
            mapped_category = ""
            for target_cat, mapped_cat in self.target_categories.items():
                if target_cat.lower() in category.lower():
                    mapped_category = mapped_cat
                    break
            
            if not mapped_category:
                return None
            
            # 소스 링크 추출
            source_link = None
            source_links = soup.find_all('a', href=True)
            for link in source_links:
                href = link['href']
                if href.startswith('http') and 'rsoe-edis.org' not in href:
                    link_text = link.get_text(strip=True).lower()
                    parent_text = ""
                    if link.parent:
                        parent_text = link.parent.get_text().lower()
                    
                    if 'source' in link_text or 'source' in parent_text:
                        source_link = href
                        break
            
            if not source_link:
                for link in source_links:
                    href = link['href']
                    if href.startswith('http') and 'rsoe-edis.org' not in href:
                        source_link = href
                        break
            
            # 좌표 유효성 검사 및 정리
            latitude = fields.get("Latitude", "").strip()
            longitude = fields.get("Longitude", "").strip()
            
            try:
                if latitude:
                    lat_clean = re.sub(r'[°NSEW]', '', latitude).strip()
                    lat_float = float(lat_clean)
                    if not (-90 <= lat_float <= 90):
                        latitude = ""
                    else:
                        latitude = str(lat_float)
                if longitude:
                    lon_clean = re.sub(r'[°NSEW]', '', longitude).strip()
                    lon_float = float(lon_clean)
                    if not (-180 <= lon_float <= 180):
                        longitude = ""
                    else:
                        longitude = str(lon_float)
            except ValueError:
                latitude = ""
                longitude = ""
            
            event_data = {
                "event_id": event_id,
                "event_title": title,
                "event_category": mapped_category,
                "original_category": category,
                "source": source_link or "",
                "event_date_utc": fields.get("Event date (UTC)", "").strip(),
                "last_update_utc": fields.get("Last update (UTC)", "").strip(),
                "latitude": latitude,
                "longitude": longitude,
                "area_range": fields.get("Area range", "").strip(),
                "address": fields.get("Address/Affected area(s)", "").strip(),
                "crawled_at": datetime.now().isoformat(),
                "event_url": event_url
            }
            
            return event_data
            
        except Exception as e:
            print(f"⚠️ Error extracting details from {event_url}: {e}")
            return None
    
    def crawl_events(self):
        """메인 크롤링 함수"""
        print("=== STARTING RSOE EDIS EVENT CRAWLING ===")
        print(f"Target categories: {list(self.target_categories.keys())}")
        print("=" * 60)
        
        try:
            # 메인 페이지 로드
            print("✓ Loading main event list page...")
            main_html = self.get_page_content(self.event_list_url)
            if not main_html:
                print("✗ Failed to load main page")
                return False
            
            # 모든 이벤트 링크 추출
            all_event_links = self.extract_all_event_links(main_html)
            
            # 추가 페이지들도 확인 (최대 3페이지까지만)
            print("✓ Looking for additional pages...")
            pagination_links = self.find_pagination_links(main_html)
            
            if pagination_links:
                print(f"✓ Found {len(pagination_links)} pagination links")
                max_pages = min(len(pagination_links), 3)  # GitHub Actions 시간 제한 고려
                
                for i, page_url in enumerate(pagination_links[:max_pages]):
                    try:
                        print(f"  → Loading additional page {i+1}/{max_pages}...")
                        page_html = self.get_page_content(page_url)
                        if page_html:
                            page_links = self.extract_all_event_links(page_html)
                            all_event_links.extend(page_links)
                        time.sleep(1)  # 서버 부하 방지
                    except Exception as e:
                        print(f"⚠️ Error loading page {page_url}: {e}")
            
            all_event_links = list(set(all_event_links))
            print(f"✓ Total unique event links collected: {len(all_event_links)}")
            
            if not all_event_links:
                print("✗ No event links found!")
                return False
            
            print("=" * 60)
            print("✓ Processing event detail pages...")
            print("=" * 60)
            
            # 각 이벤트의 상세 정보 수집
            target_events_found = 0
            max_events_to_process = min(len(all_event_links), 100)  # 10분 주기이므로 제한
            
            for i, event_url in enumerate(all_event_links[:max_events_to_process], 1):
                if i % 10 == 0:
                    print(f"[{i:3d}/{max_events_to_process}] Progress: {i/max_events_to_process*100:.1f}%")
                
                event_data = self.extract_event_details(event_url)
                if event_data:
                    self.collected_events.append(event_data)
                    target_events_found += 1
                    if i <= 5:  # 처음 5개만 상세 로그
                        print(f"  ✓ [{i:3d}] COLLECTED: {event_data['event_id']} - {event_data['event_title'][:50]}...")
                
                # 너무 빠르면 차단될 수 있으므로 적당한 딜레이
                time.sleep(0.3)
            
            print("\n" + "=" * 60)
            print(f"✓ CRAWLING COMPLETED!")
            print(f"Total processed: {max_events_to_process} events")
            print(f"Target events collected: {len(self.collected_events)} events")
            
            # 카테고리별 통계
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
    
    def save_to_json(self, filename="rsoe_events.json"):
        """수집된 데이터를 JSON 파일로 저장"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.collected_events, f, ensure_ascii=False, indent=2)
            print(f"✓ Data saved to {filename}")
            return True
        except Exception as e:
            print(f"✗ Error saving to JSON: {e}")
            return False

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("🌍 RSOE DISASTER DATA CRAWLER WITH CUMULATIVE MERGE")
    print("=" * 80)
    
    try:
        # 백업 생성
        print("1. Creating backup of existing data...")
        create_backup_if_needed("docs/data/events.json")
        
        # 크롤러 초기화 및 실행
        print("\n2. Initializing crawler...")
        crawler = RSOECrawler()
        
        print("\n3. Starting crawling process...")
        success = crawler.crawl_events()
        
        if success and crawler.collected_events:
            print(f"\n4. Successfully collected {len(crawler.collected_events)} new events")
            
            # 카테고리별 통계 출력
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
            crawler.collected_events = []  # 빈 리스트로 설정
        
        # 기존 데이터와 병합 (새 이벤트가 없어도 실행)
        print(f"\n5. Merging with existing data...")
        merged = merge_events(
            crawler.collected_events, 
            existing_path="docs/data/events.json",
            past_events_path="docs/data/past_events.json"
        )
        
        # 병합된 결과 저장
        print(f"\n6. Saving merged results...")
        output_file = "rsoe_events.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        
        print(f"✓ Final merged events saved to {output_file}: {len(merged)} total events")
        
        # 최종 통계
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
            
            print(f"\n=== FINAL DATABASE SUMMARY ===")
            print(f"📈 Total events in database: {len(merged)}")
            print(f"🆕 Events from last 7 days: {latest_events}")
            print(f"📊 Categories in database:")
            for category, count in sorted(final_categories.items()):
                print(f"  {category}: {count}")
        
        print(f"\n✅ PROCESS COMPLETED SUCCESSFULLY!")
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