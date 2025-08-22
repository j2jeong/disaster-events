import requests
from bs4 import BeautifulSoup
import json
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
import sys
import os

class RSOECrawler:
    def __init__(self):
        self.base_url = "https://rsoe-edis.org"
        self.event_list_url = "https://rsoe-edis.org/eventList"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
                print(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    print(f"Failed to fetch {url} after {retries} attempts")
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
        
        print(f"Found {len(event_links)} total event links on this page")
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
            
            print(f"Event {event_id}: '{title}' - Category: '{category}'")
            
            # 카테고리 매핑 및 필터링
            mapped_category = ""
            for target_cat, mapped_cat in self.target_categories.items():
                if target_cat.lower() in category.lower():
                    mapped_category = mapped_cat
                    break
            
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
            print(f"Error extracting details from {event_url}: {e}")
            return None
    
    def crawl_events(self):
        """메인 크롤링 함수"""
        print("Starting RSOE EDIS event crawling...")
        print(f"Target categories: {list(self.target_categories.keys())}")
        print("=" * 60)
        
        try:
            # 메인 페이지 로드
            print("Loading main event list page...")
            main_html = self.get_page_content(self.event_list_url)
            if not main_html:
                print("Failed to load main page")
                return False
            
            # 모든 이벤트 링크 추출
            all_event_links = self.extract_all_event_links(main_html)
            
            # 추가 페이지들도 확인 (최대 3페이지까지만)
            print("Looking for additional pages...")
            pagination_links = self.find_pagination_links(main_html)
            
            if pagination_links:
                print(f"Found {len(pagination_links)} pagination links")
                max_pages = min(len(pagination_links), 3)  # GitHub Actions 시간 제한 고려
                
                for i, page_url in enumerate(pagination_links[:max_pages]):
                    try:
                        print(f"Loading additional page {i+1}/{max_pages}...")
                        page_html = self.get_page_content(page_url)
                        if page_html:
                            page_links = self.extract_all_event_links(page_html)
                            all_event_links.extend(page_links)
                        time.sleep(1)
                    except Exception as e:
                        print(f"Error loading page {page_url}: {e}")
            
            all_event_links = list(set(all_event_links))
            print(f"Total unique event links collected: {len(all_event_links)}")
            
            if not all_event_links:
                print("No event links found!")
                return False
            
            print("=" * 60)
            print("Processing event detail pages...")
            print("=" * 60)
            
            # 각 이벤트의 상세 정보 수집
            target_events_found = 0
            max_events_to_process = min(len(all_event_links), 100)  # 시간 제한을 위해 최대 100개
            
            for i, event_url in enumerate(all_event_links[:max_events_to_process], 1):
                print(f"[{i:3d}/{max_events_to_process}] Processing: {event_url}")
                
                event_data = self.extract_event_details(event_url)
                if event_data:
                    self.collected_events.append(event_data)
                    target_events_found += 1
                    print(f"  ✓ COLLECTED! ({target_events_found} total)")
                
                time.sleep(0.5)  # 서버 부하 방지
            
            print("\n" + "=" * 60)
            print(f"CRAWLING COMPLETED!")
            print(f"Total processed: {max_events_to_process} events")
            print(f"Target events collected: {len(self.collected_events)} events")
            print("=" * 60)
            return True
            
        except Exception as e:
            print(f"Error during crawling: {e}")
            return False
    
    def save_to_json(self, filename="rsoe_events.json"):
        """수집된 데이터를 JSON 파일로 저장"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.collected_events, f, ensure_ascii=False, indent=2)
            print(f"Data saved to {filename}")
            return True
        except Exception as e:
            print(f"Error saving to JSON: {e}")
            return False

if __name__ == "__main__":
    crawler = RSOECrawler()
    
    # 크롤링 실행
    success = crawler.crawl_events()
    
    if success and crawler.collected_events:
        # 결과 요약 출력
        category_counts = {}
        for event in crawler.collected_events:
            category = event['event_category']
            category_counts[category] = category_counts.get(category, 0) + 1
        
        print("\n=== COLLECTION SUMMARY ===")
        print(f"Total events collected: {len(crawler.collected_events)}")
        print("\nEvents by category:")
        for category, count in sorted(category_counts.items()):
            print(f"  {category}: {count}")
        
        # JSON으로 저장
        crawler.save_to_json("rsoe_events.json")
        print("\nCrawling completed successfully!")
        sys.exit(0)
    else:
        print("\nCrawling failed or no events collected!")
        sys.exit(1)