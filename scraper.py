import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
import csv

class RSOECrawler:
    def __init__(self):
        self.base_url = "https://rsoe-edis.org"
        self.event_list_url = "https://rsoe-edis.org/eventList"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # 필터링할 카테고리들 (영어 원문 기준)
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
                print(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # 지수적 백오프
                else:
                    raise
    
    def extract_all_event_links(self, html_content):
        """이벤트 리스트에서 모든 이벤트 링크들 추출 (필터링 없이)"""
        soup = BeautifulSoup(html_content, 'html.parser')
        event_links = []
        
        # details 패턴이 포함된 모든 링크 찾기
        links = soup.find_all('a', href=re.compile(r'/eventList/details/\d+'))
        
        for link in links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                event_links.append(full_url)
        
        print(f"Found {len(event_links)} total event links on this page")
        return list(set(event_links))  # 중복 제거
    
    def find_pagination_links(self, html_content):
        """페이지네이션 링크들 찾기"""
        soup = BeautifulSoup(html_content, 'html.parser')
        page_urls = []
        
        # 페이지네이션 링크 찾기
        pagination_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        
        for link in pagination_links:
            href = link.get('href')
            if href:
                full_url = urljoin(self.base_url, href)
                page_urls.append(full_url)
        
        # 숫자로 된 페이지 링크들도 찾기
        numeric_links = soup.find_all('a', href=True)
        for link in numeric_links:
            href = link.get('href')
            if href and re.search(r'[?&]page=\d+', href):
                full_url = urljoin(self.base_url, href)
                if full_url not in page_urls:
                    page_urls.append(full_url)
        
        return list(set(page_urls))
    
    def extract_event_details(self, event_url):
        """개별 이벤트의 상세 정보 추출"""
        try:
            html_content = self.get_page_content(event_url)
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 이벤트 ID 추출 (URL에서)
            event_id_match = re.search(r'/details/(\d+)', event_url)
            event_id = event_id_match.group(1) if event_id_match else ""
            
            # 정보 추출을 위한 필드 매핑
            fields = {}
            
            # 방법 1: dt/dd 태그 쌍으로 정보 추출
            dt_elements = soup.find_all('dt')
            for dt in dt_elements:
                field_name = dt.get_text(strip=True)
                dd = dt.find_next_sibling('dd')
                if dd:
                    field_value = dd.get_text(strip=True)
                    fields[field_name] = field_value
            
            # 방법 2: 테이블 구조에서 정보 추출
            if not fields:
                rows = soup.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        field_name = cells[0].get_text(strip=True)
                        field_value = cells[1].get_text(strip=True)
                        if field_name and field_value:
                            fields[field_name] = field_value
            
            # 방법 3: 텍스트 패턴 매칭으로 추출 (가장 강력한 방법)
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
            
            # 추출된 정보 확인
            title = fields.get("Event title", "").strip()
            category = fields.get("Event category", "").strip()
            
            print(f"Event {event_id}: '{title}' - Category: '{category}'")
            
            # 카테고리 매핑 및 필터링 (이제 여기서 필터링!)
            mapped_category = ""
            for target_cat, mapped_cat in self.target_categories.items():
                if target_cat.lower() in category.lower():
                    mapped_category = mapped_cat
                    break
            
            # 타겟 카테고리가 아니면 None 반환
            if not mapped_category:
                print(f"  -> Skipping: Category '{category}' not in target list")
                return None
            
            print(f"  -> Collecting: Target category '{mapped_category}' found!")
            
            # 소스 링크 추출
            source_link = None
            source_links = soup.find_all('a', href=True)
            for link in source_links:
                href = link['href']
                if href.startswith('http') and 'rsoe-edis.org' not in href:
                    # "Source link" 텍스트 근처의 링크 우선
                    link_text = link.get_text(strip=True).lower()
                    parent_text = ""
                    if link.parent:
                        parent_text = link.parent.get_text().lower()
                    
                    if 'source' in link_text or 'source' in parent_text:
                        source_link = href
                        break
            
            # 일반적인 외부 링크도 찾기
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
                    # 도 기호나 방향 문자 제거
                    lat_clean = re.sub(r'[°NSEW]', '', latitude).strip()
                    lat_float = float(lat_clean)
                    if not (-90 <= lat_float <= 90):
                        latitude = ""
                    else:
                        latitude = str(lat_float)
                if longitude:
                    # 도 기호나 방향 문자 제거
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
                "source": source_link,
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
            print(f"✗ Error extracting details from {event_url}: {e}")
            return None
    
    def crawl_events(self):
        """메인 크롤링 함수 - 모든 링크 수집 후 세부페이지에서 필터링"""
        print("Starting RSOE EDIS event crawling...")
        print(f"Target categories: {list(self.target_categories.keys())}")
        print("Strategy: Collect ALL event links first, then filter in detail pages")
        print("=" * 60)
        
        try:
            # 1단계: 메인 이벤트 리스트 페이지에서 모든 링크 수집
            print("Step 1: Loading main event list page...")
            main_html = self.get_page_content(self.event_list_url)
            
            # 모든 이벤트 링크 추출 (필터링 없이)
            all_event_links = self.extract_all_event_links(main_html)
            
            # 2단계: 추가 페이지들도 확인
            print("Step 2: Looking for additional pages...")
            pagination_links = self.find_pagination_links(main_html)
            
            if pagination_links:
                print(f"Found {len(pagination_links)} pagination links")
                # 최대 페이지 수 제한 (무한 루프 방지)
                max_pages = min(len(pagination_links), 5)  # 최대 5페이지까지
                
                for i, page_url in enumerate(pagination_links[:max_pages]):
                    try:
                        print(f"Loading additional page {i+1}/{max_pages}...")
                        page_html = self.get_page_content(page_url)
                        page_links = self.extract_all_event_links(page_html)
                        all_event_links.extend(page_links)
                        time.sleep(1)  # 요청 간격 조절
                    except Exception as e:
                        print(f"Error loading page {page_url}: {e}")
            
            # 중복 제거
            all_event_links = list(set(all_event_links))
            print(f"Step 3: Total unique event links collected: {len(all_event_links)}")
            
            if not all_event_links:
                print("No event links found!")
                return
            
            print("=" * 60)
            print("Step 4: Processing each event detail page...")
            print("=" * 60)
            
            # 3단계: 각 이벤트의 상세 정보 수집 및 필터링
            target_events_found = 0
            
            for i, event_url in enumerate(all_event_links, 1):
                print(f"\n[{i:3d}/{len(all_event_links)}] Processing: {event_url}")
                
                event_data = self.extract_event_details(event_url)
                if event_data:
                    self.collected_events.append(event_data)
                    target_events_found += 1
                    print(f"  ✓ COLLECTED! ({target_events_found} total)")
                
                # 요청 간격 조절 (서버 부하 방지)
                time.sleep(0.5)
                
                # 진행 상황 요약 출력
                if i % 50 == 0:
                    print(f"\n--- Progress Summary ---")
                    print(f"Processed: {i}/{len(all_event_links)} events")
                    print(f"Collected: {target_events_found} target events")
                    print(f"Success rate: {target_events_found/i*100:.1f}%")
                    print("-" * 25)
            
            print("\n" + "=" * 60)
            print(f"CRAWLING COMPLETED!")
            print(f"Total processed: {len(all_event_links)} events")
            print(f"Target events collected: {len(self.collected_events)} events")
            print("=" * 60)
            
        except Exception as e:
            print(f"Error during crawling: {e}")
            import traceback
            traceback.print_exc()
    
    def save_to_json(self, filename="data/rsoe_events.json"):
        """수집된 데이터를 JSON 파일로 저장"""
        # data 디렉토리가 없으면 생성
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_events, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {filename}")

    def save_to_csv(self, filename="data/rsoe_events.csv"):
        """수집된 데이터를 CSV 파일로 저장"""
        if not self.collected_events:
            print("No data to save")
            return
        
        fieldnames = [
            "event_id", "event_title", "event_category", "original_category",
            "source", "event_date_utc", "last_update_utc", "latitude", "longitude",
            "area_range", "address", "crawled_at", "event_url"
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.collected_events)
        print(f"Data saved to {filename}")
    
    def print_summary(self):
        """수집된 데이터 요약 출력"""
        if not self.collected_events:
            print("No events collected")
            return
        
        category_counts = {}
        for event in self.collected_events:
            category = event['event_category']
            category_counts[category] = category_counts.get(category, 0) + 1
        
        print("\n=== COLLECTION SUMMARY ===")
        print(f"Total events collected: {len(self.collected_events)}")
        print("\nEvents by category:")
        for category, count in sorted(category_counts.items()):
            print(f"  {category}: {count}")
        
        # 좌표가 있는 이벤트 수
        events_with_coords = sum(1 for event in self.collected_events 
                               if event['latitude'] and event['longitude'])
        print(f"\nEvents with valid coordinates: {events_with_coords}")
        
        # 소스 링크가 있는 이벤트 수
        events_with_source = sum(1 for event in self.collected_events 
                               if event['source'])
        print(f"Events with source links: {events_with_source}")

# 사용 예시
if __name__ == "__main__":
    crawler = RSOECrawler()
    
    # 크롤링 실행 (이 부분은 new.py와 동일하게 유지)
    crawler.crawl_events()
    
    # 결과 요약 출력
    crawler.print_summary()
    
    # 데이터 저장 경로를 "data/rsoe_events.json" 으로 지정
    crawler.save_to_json("data/rsoe_events.json")
    
    print("\nCrawling completed successfully!")
