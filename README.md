# 🌍 글로벌 재해 모니터링 시스템

실시간 재해 발생 현황을 자동으로 수집하고 시각화하는 대시보드 시스템

## 🚀 주요 기능

- **자동 데이터 수집**: 매일 오전 8시 RSOE EDIS 사이트에서 재해 정보 크롤링
- **실시간 지도 시각화**: Leaflet을 활용한 인터랙티브 지도
- **시간별 애니메이션**: 시간 순서대로 재해 발생 현황 재생
- **위험 지역 감지**: 클러스터링 알고리즘으로 고위험 지역 자동 탐지
- **다양한 뷰**: 지도, 테이블, 리스트 뷰 제공
- **데이터 내보내기**: Excel, CSV 형식으로 데이터 다운로드
- **반응형 디자인**: 모바일과 데스크톱 모두 지원

## 📊 모니터링 재해 유형

- 지진 (Earthquake)
- 화산 폭발 (Volcanic eruption)
- 홍수 (Flood)
- 화재 (Fire in built environment)
- 폭발 사고 (Industrial/Surroundings explosion)
- 산사태 (Landslide)
- 전쟁 (War)
- 환경 오염 (Environment pollution)

## 🔧 기술 스택

### 프론트엔드
- HTML5, CSS3, JavaScript (ES6+)
- Leaflet.js (지도 시각화)
- SheetJS (Excel 다운로드)

### 백엔드/크롤링
- Python 3.9+
- Requests (HTTP 클라이언트)
- BeautifulSoup4 (HTML 파싱)

### 배포/자동화
- GitHub Actions (CI/CD)
- GitHub Pages (정적 호스팅)

## 🏗 설치 및 실행

### 1. 레포지토리 클론
```bash
git clone https://github.com/your-username/disaster-monitoring.git
cd disaster-monitoring
```

### 2. GitHub Pages 설정
1. 레포지토리 → Settings → Pages
2. Source: "Deploy from a branch"
3. Branch: "main", Folder: "/docs"

### 3. 자동화 활성화
GitHub Actions가 자동으로 실행되어 매일 데이터를 업데이트합니다.

### 4. 접속
`https://your-username.github.io/disaster-monitoring/`

## 📁 프로젝트 구조

```
disaster-monitoring/
├── .github/
│   └── workflows/
│       └── update-data.yml          # 자동화 워크플로우
├── crawler/
│   ├── requirements.txt             # Python 의존성
│   └── rsoe_crawler.py             # 크롤링 스크립트
├── docs/                           # GitHub Pages
│   ├── index.html                  # 메인 HTML
│   ├── styles.css                  # 스타일시트
│   ├── script.js                   # JavaScript 로직
│   └── data/
│       ├── events.json             # 재해 데이터 (자동 생성)
│       └── last_update.txt         # 마지막 업데이트 시간
├── README.md
└── .gitignore
```

## ⏰ 자동화 일정

- **실행 시간**: 매일 오전 8시 (한국 시간)
- **크롤링 범위**: 최신 재해 이벤트 최대 100건
- **업데이트 주기**: 24시간
- **실패 시 처리**: 이전 데이터 유지, 로그 기록

## 🔍 수동 실행

긴급히 데이터를 업데이트하려면:

1. 레포지토리 → Actions 탭
2. "Update Disaster Data" 워크플로우 선택
3. "Run workflow" 버튼 클릭

## 📊 데이터 소스

- **RSOE EDIS**: https://rsoe-edis.org/eventList
- **업데이트 빈도**: 실시간 (사이트 기준)
- **데이터 범위**: 전세계 재해 이벤트
- **좌표 정보**: 위도/경도 포함

## 🚨 위험 알림 시스템

시스템은 다음 조건에서 위험 알림을 표시합니다:
- 30일 내 같은 지역(반경 55km)에서 5건 이상 재해 발생
- 고위험 지역에 특별한 시각적 효과 표시
- 애니메이션으로 주의 환기

## 🎯 사용법

### 기본 조작
- **지도 탐색**: 마우스 드래그/휠로 이동/확대
- **마커 클릭**: 재해 상세 정보 팝업
- **뷰 전환**: 지도/테이블/리스트 버튼으로 전환

### 필터링
- **카테고리 필터**: 특정 재해 유형만 표시
- **날짜 범위**: 기간별 필터링
- **검색**: 제목 키워드 검색
- **시간 슬라이더**: 시간 순서대로 필터링

### 애니메이션
- **재생/정지**: ▶ 버튼으로 제어
- **속도 조절**: 느림/보통/빠름 선택
- **키보드**: 스페이스바로 재생/정지, ESC로 정지

## 📈 성능 최적화

- **데이터 캐싱**: 브라우저 캐시 활용
- **이미지 최적화**: CDN 사용
- **반응형**: 모바일 최적화
- **로딩**: 프로그레시브 로딩

## 🔒 보안 및 제한사항

- **Rate Limiting**: 크롤링 요청 간격 조절 (0.5초)
- **Timeout**: 30초 요청 타임아웃
- **Retry Logic**: 실패 시 3회 재시도
- **Error Handling**: 예외 상황 로깅 및 처리

## 🤝 기여하기

1. Fork 프로젝트
2. 피처 브랜치 생성 (`git checkout -b feature/AmazingFeature`)
3. 변경사항 커밋 (`git commit -m 'Add some AmazingFeature'`)
4. 브랜치에 Push (`git push origin feature/AmazingFeature`)
5. Pull Request 생성

## 📝 라이센스

이 프로젝트는 MIT 라이센스를 따릅니다.

## 🙋‍♂️ 문의사항

- GitHub Issues: 버그 리포트 및 기능 요청
- Actions 로그: 크롤링 실행 상태 확인

## 📋 체크리스트

설정 완료 후 확인사항:

- [ ] GitHub Pages 활성화 확인
- [ ] Actions 권한 설정 (Read and write permissions)
- [ ] 첫 번째 자동 실행 완료 확인
- [ ] 웹사이트 접속 및 데이터 표시 확인
- [ ] 모든 기능 정상 동작 확인

---

**⚠ 주의**: 이 시스템은 교육 및 연구 목적으로 제작되었습니다. 실제 재해 대응에는 공식 기관의 정보를 활용하시기 바랍니다.