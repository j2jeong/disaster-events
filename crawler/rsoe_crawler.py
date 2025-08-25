import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import sys
import os
import pathlib
from typing import Dict, Any, List

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
    """중복 제거용 키 생성 (정규화)"""
    clean_title = re.sub(r'[^\w\s]', '', title.lower()).strip()
    clean_title = re.sub(r'\s+', ' ', clean_title)
    
    try:
        lat_clean = f"{float(lat):.4f}" if lat else "0"
        lon_clean = f"{float(lon):.4f}" if lon else "0"
    except:
        lat_clean = "0"
        lon_clean = "0"
    
    date_clean = date[:10] if len(date) >= 10 else date
    
    return f"{clean_title}|{date_clean}|{lat_clean}|{lon_clean}"


def merge_events(new_events: List[Dict[str, Any]], 
                 existing_path: str = "docs/data/events.json",
                 past_events_path: str = "docs/data/past_events.json") -> List[Dict[str, Any]]:
    """
    개선된 누적 데이터 병합 시스템
    - GitHub Actions 환경에 최적화
    - 안정적인 백업 및 복구 시스템
    - 상세한 로깅 및 검증
    """
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
    
    # 파일 경로 검증 및 생성
    existing_path_obj = pathlib.Path(existing_path)
    past_path_obj = pathlib.Path(past_events_path)
    
    # 디렉토리 생성
    existing_path_obj.parent.mkdir(parents=True, exist_ok=True)
    past_path_obj.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 Working directories:")
    print(f"  - Current events: {existing_path}")
    print(f"  - Past events: {past_events_path}")
    print(f"  - New events provided: {len(new_events)}")
    
    try:
        # 1. 과거 이벤트 데이터 로드 (최고 우선순위 - 보존되어야 함)
        print(f"\n📚 Loading archived events from {past_events_path}...")
        if past_path_obj.exists() and past_path_obj.stat().st_size > 0:
            try:
                past_content = past_path_obj.read_text(encoding="utf-8")
                if past_content.strip():
                    past_events = json.loads(past_content)
                    print(f"✅ Successfully loaded {len(past_events)} archived events")
                    
                    for ev in past_events:
                        event_id = str(ev.get("event_id", "")).strip()
                        if event_id and event_id != "":
                            # 과거 이벤트는 무조건 보존
                            merged[event_id] = ev
                            stats['past_events_loaded'] += 1
                            # 아카이브 플래그 추가
                            merged[event_id]['_archived'] = True
                else:
                    print(f"⚠️ {past_events_path} exists but is empty")
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error in past events: {e}")
                # 백업 생성
                backup_path = past_path_obj.with_suffix('.json.backup')
                past_path_obj.rename(backup_path)
                print(f"📦 Corrupted file backed up to {backup_path}")
            except Exception as e:
                print(f"⚠️ Error loading past events: {e}")
        else:
            print(f"📝 No existing archived events file found")
        
        # 2. 현재 이벤트 데이터 로드
        print(f"\n📂 Loading current events from {existing_path}...")
        if existing_path_obj.exists() and existing_path_obj.stat().st_size > 0:
            try:
                existing_content = existing_path_obj.read_text(encoding="utf-8")
                if existing_content.strip():
                    existing_events = json.loads(existing_content)
                    print(f"✅ Successfully loaded {len(existing_events)} current events")
                    
                    for ev in existing_events:
                        event_id = str(ev.get("event_id", "")).strip()
                        if event_id and event_id != "":
                            existing_in_merged = merged.get(event_id)
                            
                            if existing_in_merged and existing_in_merged.get('_archived'):
                                # 이미 아카이브된 이벤트는 건드리지 않음
                                print(f"  📚 Preserving archived event: {event_id}")
                                continue
                            elif existing_in_merged:
                                # 기존 것이 더 최신이면 유지
                                existing_time = _parse_iso(ev.get("crawled_at", ""))
                                merged_time = _parse_iso(existing_in_merged.get("crawled_at", ""))
                                if existing_time > merged_time:
                                    merged[event_id] = ev
                            else:
                                merged[event_id] = ev
                                stats['existing_events_loaded'] += 1
                else:
                    print(f"⚠️ {existing_path} exists but is empty")
            except json.JSONDecodeError as e:
                print(f"❌ JSON parsing error in current events: {e}")
                stats['validation_errors'] += 1
            except Exception as e:
                print(f"⚠️ Error loading current events: {e}")
        else:
            print(f"📝 No existing current events file found")
        
        # 3. 새로운 데이터 통합
        print(f"\n🆕 Processing {len(new_events)} new events...")
        
        for i, ev in enumerate(new_events, 1):
            if i % 50 == 0:
                print(f"  ⏳ Processing event {i}/{len(new_events)}...")
            
            # 데이터 검증
            if not isinstance(ev, dict):
                stats['validation_errors'] += 1
                continue
            
            event_id = str(ev.get("event_id", "")).strip()
            
            # event_id가 없는 경우 임시 ID 생성
            if not event_id or event_id == "":
                title = str(ev.get('event_title', '')).strip()[:50]
                date = str(ev.get('event_date_utc', '')).strip()[:10]
                lat = str(ev.get('latitude', '')).strip()[:10]
                lon = str(ev.get('longitude', '')).strip()[:10]
                
                if not title:  # 제목도 없으면 스킵
                    stats['validation_errors'] += 1
                    continue
                
                event_id = f"TEMP_{hash(f'{title}_{date}_{lat}_{lon}')}"
                ev["event_id"] = event_id
            
            # 기존 이벤트 확인
            existing = merged.get(event_id)
            
            if existing and existing.get('_archived'):
                # 아카이브된 이벤트는 건드리지 않음
                continue
            elif existing:
                # 더 최신 것으로 업데이트
                new_time = _parse_iso(ev.get("crawled_at", ""))
                existing_time = _parse_iso(existing.get("crawled_at", ""))
                
                if new_time >= existing_time:
                    merged[event_id] = ev
                    stats['events_updated'] += 1
                    if i <= 10:  # 처음 10개만 로그
                        print(f"  🔄 Updated: {event_id}")
            else:
                # 완전히 새로운 이벤트
                merged[event_id] = ev
                stats['new_events_added'] += 1
                if i <= 10:  # 처음 10개만 로그
                    print(f"  ✨ New: {event_id}")
        
        print(f"✅ ID-based merge completed: {len(merged)} unique events")
        
        # 4. 보조 키로 중복 제거 (서로 다른 ID지만 실질적으로 같은 이벤트)
        print(f"\n🔍 Performing content-based deduplication...")
        seen_keys = set()
        deduped = []
        duplicate_count = 0
        
        # 아카이브된 것들을 먼저 처리 (우선순위 보장)
        archived_events = [ev for ev in merged.values() if ev.get('_archived')]
        non_archived_events = [ev for ev in merged.values() if not ev.get('_archived')]
        
        for ev in archived_events + non_archived_events:
            # 필수 데이터 검증
            title = str(ev.get('event_title', '')).strip()
            if not title:
                stats['validation_errors'] += 1
                continue
            
            # 중복 검사 키 생성
            date = str(ev.get('event_date_utc', '')).strip()
            lat = str(ev.get('latitude', '')).strip()
            lon = str(ev.get('longitude', '')).strip()
            
            key = clean_duplicate_key(title, date, lat, lon)
            
            if key not in seen_keys:
                seen_keys.add(key)
                # _archived 플래그 제거 (출력용)
                if '_archived' in ev:
                    ev_copy = ev.copy()
                    del ev_copy['_archived']
                    deduped.append(ev_copy)
                else:
                    deduped.append(ev)
            else:
                duplicate_count += 1
        
        stats['duplicates_removed'] = duplicate_count
        print(f"✅ Content-based deduplication: removed {duplicate_count} duplicates")
        
        # 5. 시간별 아카이빙 시스템 (30일 이상된 것들을 past_events.json으로 이동)
        cutoff_date = datetime.now() - timedelta(days=30)
        recent_events = []
        old_events = []
        
        print(f"\n📅 Separating events by age (cutoff: {cutoff_date.strftime('%Y-%m-%d')})...")
        
        for event in deduped:
            try:
                # crawled_at 기준으로 판단
                crawl_time_str = event.get('crawled_at', '')
                if crawl_time_str:
                    crawl_time = datetime.fromisoformat(crawl_time_str.replace('Z', '+00:00'))
                    if crawl_time >= cutoff_date:
                        recent_events.append(event)
                    else:
                        old_events.append(event)
                else:
                    # crawled_at이 없으면 최신으로 간주
                    recent_events.append(event)
            except Exception as e:
                # 날짜 파싱 실패하면 최신으로 간주
                recent_events.append(event)
        
        stats['old_events_archived'] = len(old_events)
        print(f"  📊 Recent events (keep in main): {len(recent_events)}")
        print(f"  📚 Old events (move to archive): {len(old_events)}")
        
        # 6. 과거 이벤트 파일 업데이트
        if old_events:
            print(f"\n📚 Updating archived events file...")
            
            # 기존 아카이브와 병합
            try:
                existing_past = []
                if past_path_obj.exists():
                    past_content = past_path_obj.read_text(encoding="utf-8")
                    if past_content.strip():
                        existing_past = json.loads(past_content)
                
                # 아카이브 데이터도 중복 제거
                archive_merged = {}
                for ev in existing_past + old_events:
                    event_id = ev.get("event_id")
                    if event_id:
                        existing_archive = archive_merged.get(event_id)
                        if not existing_archive:
                            archive_merged[event_id] = ev
                        else:
                            # 더 최신 것으로 유지
                            new_time = _parse_iso(ev.get("crawled_at", ""))
                            existing_time = _parse_iso(existing_archive.get("crawled_at", ""))
                            if new_time > existing_time:
                                archive_merged[event_id] = ev
                
                final_archive = list(archive_merged.values())
                final_archive.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
                
                # 아카이브 파일 저장
                with open(past_path_obj, 'w', encoding='utf-8') as f:
                    json.dump(final_archive, f, ensure_ascii=False, indent=2)
                
                print(f"✅ Updated archive with {len(final_archive)} total archived events")
                
            except Exception as e:
                print(f"❌ Error updating archive: {e}")
                stats['validation_errors'] += 1
        
        # 7. 최종 정렬 및 검증
        final_events = recent_events
        final_events.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
        
        # 최종 검증
        valid_events = []
        for event in final_events:
            if (event.get('event_title') and 
                event.get('event_id') and 
                event.get('event_category')):
                valid_events.append(event)
            else:
                stats['validation_errors'] += 1
        
        final_events = valid_events
        
        # 8. 통계 및 결과 출력
        print(f"\n" + "=" * 60)
        print(f"📊 MERGE PROCESS COMPLETED")
        print(f"=" * 60)
        print(f"Past events loaded: {stats['past_events_loaded']}")
        print(f"Existing events loaded: {stats['existing_events_loaded']}")
        print(f"New events provided: {stats['new_events_provided']}")
        print(f"New events added: {stats['new_events_added']}")
        print(f"Events updated: {stats['events_updated']}")
        print(f"Content duplicates removed: {stats['duplicates_removed']}")
        print(f"Events archived: {stats['old_events_archived']}")
        print(f"Validation errors: {stats['validation_errors']}")
        print(f"Final events in main file: {len(final_events)}")
        
        # 카테고리별 통계
        if final_events:
            category_stats = {}
            for event in final_events:
                cat = event.get('event_category', 'Unknown')
                category_stats[cat] = category_stats.get(cat, 0) + 1
            
            print(f"\n📈 Categories in final dataset:")
            for cat, count in sorted(category_stats.items()):
                print(f"  {cat}: {count}")
        
        print(f"=" * 60)
        
        return final_events
        
    except Exception as e:
        print(f"\n💥 CRITICAL ERROR in merge process: {e}")
        import traceback
        traceback.print_exc()
        
        # 비상 복구: 최소한 새 데이터라도 반환
        print(f"🔄 Emergency fallback: returning new events only")
        return new_events if new_events else []

def create_backup_if_needed(events_path: str = "docs/data/events.json"):
    """
    포괄적인 백업 시스템 (GitHub Actions용)
    """
    try:
        events_file = pathlib.Path(events_path)
        if not events_file.exists() or events_file.stat().st_size == 0:
            print("⚠️ No data to backup")
            return
        
        # 백업 디렉토리 생성
        backup_dir = pathlib.Path("docs/data/backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 타임스탬프 백업
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = backup_dir / f"events_backup_{timestamp}.json"
        
        # 백업 생성
        backup_path.write_text(events_file.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"✅ Created timestamped backup: {backup_path}")
        
        # GitHub Actions run number 백업 (환경변수 있을 때)
        run_number = os.environ.get('GITHUB_RUN_NUMBER')
        if run_number:
            run_backup_path = backup_dir / f"events_run_{run_number}.json"
            backup_path.link_to(run_backup_path.resolve())  # 하드링크 생성
            print(f"✅ Created run-based backup: {run_backup_path}")
        
        # 오래된 백업 정리 (최근 10개만 유지)
        backups = sorted(backup_dir.glob("events_backup_*.json"))
        if len(backups) > 10:
            for old_backup in backups[:-10]:
                old_backup.unlink()
                print(f"🗑️ Removed old backup: {old_backup.name}")
        
    except Exception as e:
        print(f"⚠️ Error creating backup: {e}")

def validate_data_integrity(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    데이터 무결성 검증 시스템
    """
    report = {
        'total_events': len(events),
        'valid_events': 0,
        'invalid_events': 0,
        'issues': [],
        'categories': {},
        'date_range': {},
        'coordinate_issues': 0,
        'missing_fields': {}
    }
    
    required_fields = ['event_id', 'event_title', 'event_category']
    
    for i, event in enumerate(events):
        is_valid = True
        event_issues = []
        
        # 필수 필드 검사
        for field in required_fields:
            if not event.get(field):
                is_valid = False
                event_issues.append(f"Missing {field}")
                report['missing_fields'][field] = report['missing_fields'].get(field, 0) + 1
        
        # 좌표 검증
        try:
            lat = float(event.get('latitude', 0))
            lon = float(event.get('longitude', 0))
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                is_valid = False
                event_issues.append("Invalid coordinates")
                report['coordinate_issues'] += 1
        except (ValueError, TypeError):
            is_valid = False
            event_issues.append("Invalid coordinate format")
            report['coordinate_issues'] += 1
        
        # 카테고리 통계
        category = event.get('event_category', 'Unknown')
        report['categories'][category] = report['categories'].get(category, 0) + 1
        
        if is_valid:
            report['valid_events'] += 1
        else:
            report['invalid_events'] += 1
            report['issues'].append({
                'index': i,
                'event_id': event.get('event_id', 'Unknown'),
                'issues': event_issues
            })
    
    # 날짜 범위 분석
    valid_dates = []
    for event in events:
        try:
            if event.get('event_date_utc'):
                date = datetime.fromisoformat(event['event_date_utc'].replace('Z', '+00:00'))
                valid_dates.append(date)
        except:
            pass
    
    if valid_dates:
        report['date_range'] = {
            'earliest': min(valid_dates).isoformat(),
            'latest': max(valid_dates).isoformat(),
            'span_days': (max(valid_dates) - min(valid_dates)).days
        }
    
    return report

def update_past_events_archive():
    """
    events.json의 30일 이상 된 이벤트들을 past_events.json으로 이동
    (아카이빙 시스템)
    """
    try:
        events_path = pathlib.Path("docs/data/events.json")
        past_path = pathlib.Path("docs/data/past_events.json")
        
        if not events_path.exists():
            return
        
        # 현재 이벤트들 로드
        with open(events_path, 'r', encoding='utf-8') as f:
            current_events = json.load(f)
        
        # 기존 과거 이벤트들 로드
        past_events = []
        if past_path.exists():
            try:
                with open(past_path, 'r', encoding='utf-8') as f:
                    past_events = json.load(f)
            except:
                pass
        
        # 30일 기준으로 분리
        cutoff_date = datetime.now() - timedelta(days=30)
        recent_events = []
        old_events = []
        
        for event in current_events:
            crawled_at = event.get('crawled_at', '')
            try:
                if crawled_at:
                    crawl_time = datetime.fromisoformat(crawled_at.replace('Z', '+00:00'))
                    if crawl_time >= cutoff_date:
                        recent_events.append(event)
                    else:
                        old_events.append(event)
                else:
                    recent_events.append(event)  # 날짜 없으면 최신으로 간주
            except:
                recent_events.append(event)  # 파싱 실패하면 최신으로 간주
        
        if old_events:
            print(f"✓ Archiving {len(old_events)} old events to past_events.json")
            
            # 기존 과거 이벤트들과 합치기
            all_past_events = past_events + old_events
            
            # past_events.json에서 중복 제거
            past_merged = {}
            for ev in all_past_events:
                event_id = str(ev.get("event_id", "")).strip()
                if event_id:
                    # 더 최신 것으로 유지
                    if event_id in past_merged:
                        existing_time = _parse_iso(past_merged[event_id].get("crawled_at", ""))
                        new_time = _parse_iso(ev.get("crawled_at", ""))
                        if new_time > existing_time:
                            past_merged[event_id] = ev
                    else:
                        past_merged[event_id] = ev
            
            # past_events.json 업데이트
            final_past_events = list(past_merged.values())
            final_past_events.sort(key=lambda x: _parse_iso(x.get("crawled_at", "")), reverse=True)
            
            with open(past_path, 'w', encoding='utf-8') as f:
                json.dump(final_past_events, f, ensure_ascii=False, indent=2)
            
            # events.json을 최신 이벤트들만으로 업데이트
            with open(events_path, 'w', encoding='utf-8') as f:
                json.dump(recent_events, f, ensure_ascii=False, indent=2)
            
            print(f"✓ Updated events.json with {len(recent_events)} recent events")
            print(f"✓ Updated past_events.json with {len(final_past_events)} archived events")
        
    except Exception as e:
        print(f"⚠️ Error updating past events archive: {e}")

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
        output_file = "docs/data/events.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        
        print(f"✓ Final merged events saved to {output_file}: {len(merged)} total events")
        
        # 과거 이벤트 아카이빙 (선택적 - 30일 이상된 것들은 past_events.json으로 이동)
        print(f"\n7. Updating past events archive...")
        update_past_events_archive()
        
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