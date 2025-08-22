# crawler/merge_data.py

import json
import os

def merge_and_archive():
    """
    이전의 'events.json' 데이터를 'past_events.json'으로 병합하여 아카이브합니다.
    - event_id를 기준으로 중복을 제거합니다.
    - 날짜순으로 정렬하여 파일을 저장합니다.
    """
    repo_root = os.getenv('GITHUB_WORKSPACE', '.')
    past_events_path = os.path.join(repo_root, 'docs/data/past_events.json')
    current_events_path = os.path.join(repo_root, 'docs/data/events.json')

    print("🔄 데이터 아카이브 시작...")

    # 1. 과거 데이터 로드 (파일이 없거나 비어있으면 빈 리스트로 시작)
    try:
        with open(past_events_path, 'r', encoding='utf-8') as f:
            past_events = json.load(f)
        if not isinstance(past_events, list): past_events = []
        print(f"🗂️ '{past_events_path}'에서 {len(past_events)}개의 과거 이벤트를 로드했습니다.")
    except (FileNotFoundError, json.JSONDecodeError):
        past_events = []
        print(f"⚠️ '{past_events_path}' 파일을 찾을 수 없거나 비어있어, 새 아카이브를 시작합니다.")

    # 2. 이전 실행에서 가져온 '최신' 데이터 로드
    try:
        with open(current_events_path, 'r', encoding='utf-8') as f:
            events_to_archive = json.load(f)
        if not isinstance(events_to_archive, list): events_to_archive = []
        print(f"📑 '{current_events_path}'에서 {len(events_to_archive)}개의 이벤트를 아카이브 대상으로 찾았습니다.")
    except (FileNotFoundError, json.JSONDecodeError):
        events_to_archive = []
        print(f"ℹ️ '{current_events_path}' 파일이 없어 아카이브할 새 데이터가 없습니다.")

    if not events_to_archive:
        print("✅ 아카이브할 새 이벤트가 없습니다. 작업을 종료합니다.")
        return

    # 3. event_id를 키로 사용하여 중복 제거 및 병합
    # 이렇게 하면 동일 ID의 이벤트는 최신 정보로 덮어써집니다.
    events_map = {event['event_id']: event for event in past_events if event.get('event_id')}
    for event in events_to_archive:
        if event.get('event_id'):
            events_map[event['event_id']] = event
    
    merged_events = list(events_map.values())

    # 4. 날짜(event_date_utc) 기준으로 내림차순 정렬
    merged_events.sort(key=lambda x: x.get('event_date_utc', ''), reverse=True)

    # 5. 결과를 다시 past_events.json에 저장
    try:
        os.makedirs(os.path.dirname(past_events_path), exist_ok=True)
        with open(past_events_path, 'w', encoding='utf-8') as f:
            json.dump(merged_events, f, ensure_ascii=False, indent=2)
        print(f"✅ 성공! 총 {len(merged_events)}개의 이벤트가 '{past_events_path}'에 저장되었습니다.")
    except Exception as e:
        print(f"❌ 아카이브 파일 저장 중 오류 발생: {e}")
        exit(1)

if __name__ == "__main__":
    merge_and_archive()