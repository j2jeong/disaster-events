// 전역 변수
let disasterEvents = [];

// 디버깅용 전역 함수
window.debugLoadData = async function() {
    console.log('🔍 Debug: Starting basic data load test...');
    try {
        const response = await fetch('./data/events.json?t=' + new Date().getTime());
        console.log('Response status:', response.status);
        console.log('Response headers:', Array.from(response.headers.entries()));

        if (!response.ok) {
            console.error('HTTP Error:', response.status, response.statusText);
            return false;
        }

        const text = await response.text();
        console.log('Response length:', text.length);

        const json = JSON.parse(text);
        console.log('JSON parsed successfully. Events count:', json.length);

        return json;
    } catch (error) {
        console.error('Debug load failed:', error);
        return false;
    }
};

// 특정 RSOE 이벤트 좌표 디버깅
window.debugRsoeCoordinates = async function() {
    console.log('🔍 Debugging RSOE coordinates...');
    try {
        const data = await debugLoadData();
        if (!data) return;

        const problemEventIds = ['167359', '316381', '489963', '558581', '590400'];

        console.log('📊 Checking problematic RSOE events:');
        problemEventIds.forEach(eventId => {
            const event = data.find(e => e.event_id === eventId);
            if (event) {
                console.log(`🔍 Event ${eventId}:`, {
                    title: event.title,
                    latitude: event.latitude,
                    longitude: event.longitude,
                    address: event.address,
                    data_source: event.data_source || 'rsoe'
                });
            } else {
                console.log(`❌ Event ${eventId} not found in data`);
            }
        });

        // Check all RSOE events with 0 coordinates
        const rsoeEvents = data.filter(e => !e.data_source || e.data_source === 'rsoe');
        const zeroCoordEvents = rsoeEvents.filter(e =>
            parseFloat(e.latitude || 0) === 0 && parseFloat(e.longitude || 0) === 0
        );

        console.log(`📊 Total RSOE events: ${rsoeEvents.length}`);
        console.log(`⚠️ RSOE events with 0 coordinates: ${zeroCoordEvents.length}`);

        if (zeroCoordEvents.length > 0) {
            console.log('🔍 Zero coordinate events:');
            zeroCoordEvents.slice(0, 10).forEach(event => {
                console.log(`  - ${event.event_id}: ${event.title} (${event.address})`);
            });
        }

    } catch (error) {
        console.error('Debug failed:', error);
    }
};
let filteredData = [];
let sortedData = [];
let map;
let markersLayer;
let currentView = 'map';
let animationInterval = null;
let animationSpeed = 700;
let isPlaying = false;
let currentTimeIndex = 0;
let highlightedMarker = null;
let riskAnimationMarkers = [];
let currentSort = { column: null, direction: null };
let lastUpdateTime = null;
let tableItemsToShow = 50;
let activeDateRange = null;

// 테스트 데이터 로딩 함수
async function loadTestData() {
    console.log('🧪 Loading test data...');
    try {
        const response = await fetch('./test_data.json');
        const testData = await response.json();

        disasterEvents = testData.map(event => ({
            ...event,
            latitude: parseFloat(event.latitude) || 0,
            longitude: parseFloat(event.longitude) || 0,
            event_date: new Date(event.event_date_utc),
            hasValidCoords: event.latitude && event.longitude && event.latitude !== "" && event.longitude !== ""
        }));

        console.log(`✅ Test data loaded: ${disasterEvents.length} events`);
        document.getElementById('loadingIndicator').classList.add('hidden');
        initializeData();
        return true;
    } catch (error) {
        console.error('❌ Test data loading failed:', error);
        return false;
    }
}

// 데이터 로딩 함수 (개선된 버전 - past_events.json도 고려)
async function loadDisasterData() {
    const loadingIndicator = document.getElementById('loadingIndicator');
    const errorMessage = document.getElementById('errorMessage');
    const refreshBtn = document.getElementById('refreshBtn');
    
    try {
        loadingIndicator.classList.remove('hidden');
        errorMessage.classList.add('hidden');
        refreshBtn.disabled = true;
        refreshBtn.textContent = '로딩 중...';
        
        console.log('🔄 Starting data loading process...');
        
        // 1. 메인 이벤트 데이터 로드
        console.log('📂 Loading main events data...');
        const response = await fetch('./data/events.json?t=' + new Date().getTime());
        console.log('📡 Fetch response:', {
            status: response.status,
            statusText: response.statusText,
            headers: Array.from(response.headers.entries()),
            url: response.url
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status} - ${response.statusText}`);
        }

        const responseText = await response.text();
        console.log(`📄 Response text length: ${responseText.length} characters`);

        let mainData;
        try {
            mainData = JSON.parse(responseText);
        } catch (parseError) {
            console.error('JSON parsing failed:', parseError);
            console.error('First 500 chars of response:', responseText.substring(0, 500));
            throw new Error(`JSON parsing failed: ${parseError.message}`);
        }
        console.log(`✅ Loaded ${mainData.length} events from main data file`);

    // Debug: Count events by source
    const sourceCount = {};
    mainData.forEach(event => {
        const source = event.data_source || getDataSourceFromEventId(event.event_id);
        sourceCount[source] = (sourceCount[source] || 0) + 1;
    });
    console.log('📊 Events by source in loaded data:', sourceCount);
        
        // 2. 과거 이벤트 데이터 로드 시도 (선택적)
        let pastData = [];
        try {
            console.log('📂 Attempting to load past events data...');
            const pastResponse = await fetch('./data/past_events.json?t=' + new Date().getTime());
            if (pastResponse.ok) {
                pastData = await pastResponse.json();
                console.log(`✅ Loaded ${pastData.length} events from past events file`);
            } else {
                console.log('⚠️ Past events file not available or empty');
            }
        } catch (e) {
            console.log('⚠️ Could not load past events:', e.message);
        }
        
        // 3. 데이터 병합 및 중복 제거
        console.log('🔄 Merging and deduplicating data...');
        const allEvents = [...pastData, ...mainData];
        const eventMap = new Map();
        
        // event_id 기준으로 중복 제거 (최신 것 우선)
        allEvents.forEach(event => {
            const eventId = event.event_id;
            if (eventId && eventId.trim() !== '') {
                const existing = eventMap.get(eventId);
                if (!existing || new Date(event.crawled_at || 0) > new Date(existing.crawled_at || 0)) {
                    eventMap.set(eventId, event);
                }
            }
        });
        
        // 보조 키로 한 번 더 중복 제거
        const deduped = [];
        const seenKeys = new Set();
        
        for (const event of eventMap.values()) {
            const title = (event.event_title || '').trim().toLowerCase().replace(/[^\w\s]/g, '').replace(/\s+/g, ' ');
            const date = (event.event_date_utc || '').substring(0, 10);
            const lat = parseFloat(event.latitude || 0).toFixed(4);
            const lon = parseFloat(event.longitude || 0).toFixed(4);
            const key = `${title}|${date}|${lat}|${lon}`;
            
            if (!seenKeys.has(key) && title) {
                seenKeys.add(key);
                deduped.push(event);
            }
        }

        console.log(`🎯 Final deduplicated events: ${deduped.length}`);
        
        // 4. 업데이트 시간 로드
        try {
            const updateResponse = await fetch('./data/last_update.txt?t=' + new Date().getTime());
            if (updateResponse.ok) {
                const updateText = await updateResponse.text();
                lastUpdateTime = updateText.replace('Last updated: ', '').trim();
            }
        } catch (e) {
            console.log('⚠️ Could not load update time:', e);
        }
        
        // 5. 데이터 전처리
        console.log('🔄 Starting data preprocessing...');

        try {
            disasterEvents = deduped.map((event, index) => {
                try {
                    // More careful coordinate parsing - preserve original values if they're valid numbers
                    let lat, lon;

                    if (event.latitude === null || event.latitude === undefined || event.latitude === "") {
                        lat = 0;
                    } else {
                        lat = parseFloat(event.latitude);
                        if (isNaN(lat)) {
                            console.warn(`⚠️ Invalid latitude for event ${event.event_id}: "${event.latitude}"`);
                            lat = 0;
                        }
                    }

                    if (event.longitude === null || event.longitude === undefined || event.longitude === "") {
                        lon = 0;
                    } else {
                        lon = parseFloat(event.longitude);
                        if (isNaN(lon)) {
                            console.warn(`⚠️ Invalid longitude for event ${event.event_id}: "${event.longitude}"`);
                            lon = 0;
                        }
                    }

                    // Fix invalid date formats like "2025-07-08T00:00:00+00:00Z"
                    let dateString = event.event_date_utc;
                    if (dateString && dateString.includes('+00:00Z')) {
                        dateString = dateString.replace('+00:00Z', 'Z');
                    }

                    const eventDate = new Date(dateString);

                    // Validate date
                    if (isNaN(eventDate.getTime())) {
                        console.warn(`⚠️ Invalid date for event ${event.event_id}: ${event.event_date_utc} (fixed: ${dateString})`);
                    }

                    return {
                        ...event,
                        latitude: lat,
                        longitude: lon,
                        event_date: isNaN(eventDate.getTime()) ? new Date() : eventDate,
                        hasValidCoords: lat !== 0 && lon !== 0 && !isNaN(lat) && !isNaN(lon)
                    };
                } catch (mapError) {
                    console.error(`❌ Error processing event ${index}:`, mapError, event);
                    return null;
                }
            }).filter(event => {
                if (!event) return false;
                // Keep all events with valid titles (coordinates are optional)
                const isValidEvent = event.event_title && event.event_title.trim() !== '';
                return isValidEvent;
            });

            const coordCount = disasterEvents.filter(e => e.hasValidCoords).length;
            console.log(`✅ Data preprocessing completed: ${disasterEvents.length} events (${coordCount} with valid coordinates)`);

            // Debug: Show sample coordinate values
            console.log('📍 Sample coordinate values:');
            disasterEvents.slice(0, 5).forEach((event, i) => {
                console.log(`  ${i+1}. ${event.event_id}: lat=${event.latitude}, lon=${event.longitude}, hasValidCoords=${event.hasValidCoords}`);
            });

        } catch (preprocessError) {
            console.error('❌ Data preprocessing failed:', preprocessError);
            throw new Error(`Data preprocessing failed: ${preprocessError.message}`);
        }

        // 시간순 정렬
        disasterEvents.sort((a, b) => a.event_date - b.event_date);
        
        // Count events with valid coordinates
        const coordCount = disasterEvents.filter(e => e.hasValidCoords).length;
        console.log(`✅ Processed ${disasterEvents.length} valid events (${coordCount} with coordinates)`);
        loadingIndicator.classList.add('hidden');
        
        // 통계 출력
        const categoryStats = {};
        const recentStats = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
        let recentCount = 0;
        
        disasterEvents.forEach(event => {
            const cat = event.event_category || 'Unknown';
            const source = event.data_source || getDataSourceFromEventId(event.event_id);

            categoryStats[cat] = (categoryStats[cat] || 0) + 1;

            if (new Date(event.crawled_at || 0) >= recentStats) {
                recentCount++;
            }
        });
        
        console.log('📊 Data Statistics:');
        console.log(`  Total events: ${disasterEvents.length}`);
        console.log(`  Recent events (7 days): ${recentCount}`);
        console.log('  Categories:');
        Object.entries(categoryStats).forEach(([cat, count]) => {
            console.log(`    ${cat}: ${count}`);
        });
        
        initializeData();
        updateLastUpdateDisplay();
        
    } catch (error) {
        console.error('❌ Failed to load disaster data:', error);
        console.error('Error details:', {
            message: error.message,
            stack: error.stack,
            name: error.name
        });

        loadingIndicator.classList.add('hidden');
        errorMessage.classList.remove('hidden');

        // 에러 메시지에 더 구체적인 정보 표시
        const errorMsgElement = document.querySelector('#errorMessage p');
        if (errorMsgElement) {
            errorMsgElement.textContent = `데이터 로딩 실패: ${error.message || '알 수 없는 오류'}. 브라우저 콘솔을 확인해주세요.`;
        }

        // 에러 발생시에도 기존 데이터라도 유지하려고 시도
        if (disasterEvents.length === 0) {
            console.log('🔄 Attempting to use test data as fallback...');
            const testSuccess = await loadTestData();
            if (testSuccess) {
                console.log('✅ Successfully loaded test data as fallback');
                return;
            }
        }
    } finally {
        refreshBtn.disabled = false;
        refreshBtn.textContent = '🔄 새로고침';
        refreshBtn.classList.remove('refreshing');
    }
}

// 마지막 업데이트 시간 표시 (개선된 버전)
function updateLastUpdateDisplay() {
    const lastUpdateElement = document.getElementById('lastUpdate');
    
    if (lastUpdateTime) {
        lastUpdateElement.textContent = `최종 업데이트: ${lastUpdateTime}`;
    } else if (disasterEvents.length > 0) {
        // crawled_at 중 가장 최신 것 찾기
        const latestCrawl = disasterEvents.reduce((latest, event) => {
            const eventCrawl = new Date(event.crawled_at || 0);
            const latestCrawl = new Date(latest || 0);
            return eventCrawl > latestCrawl ? event.crawled_at : latest;
        }, null);
        
        if (latestCrawl) {
            const crawlDate = new Date(latestCrawl);
            lastUpdateElement.textContent = `최종 업데이트: ${crawlDate.toLocaleString()}`;
        } else {
            lastUpdateElement.textContent = '업데이트 시간 불명';
        }
    } else {
        lastUpdateElement.textContent = '데이터 없음';
    }
}

// 데이터 새로고침
async function refreshData() {
    const refreshBtn = document.getElementById('refreshBtn');
    refreshBtn.classList.add('refreshing');
    refreshBtn.textContent = '새로고침 중...';

    console.log('🔄 Manual refresh requested');
    console.log('💥 Force clearing all caches...');

    // Clear any browser caches if possible
    try {
        if ('caches' in window) {
            const cacheNames = await caches.keys();
            await Promise.all(cacheNames.map(name => caches.delete(name)));
            console.log('✅ Service Worker caches cleared');
        }
    } catch (e) {
        console.log('⚠️ Could not clear service worker caches:', e);
    }

    // Force reload with timestamp
    window.location.href = window.location.href.split('?')[0] + '?refresh=' + Date.now();
}

// 통계 업데이트 함수 (개선된 버전)
function updateStats() {
    const stats = document.getElementById('stats');
    if (!stats) return;
    
    const totalEvents = filteredData.length;
    const categories = [...new Set(filteredData.map(event => event.event_category))];
    
    // 최근 7일 이벤트
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const recentEvents = filteredData.filter(event => {
        return event.event_date >= sevenDaysAgo;
    });
    
    // 최근 30일 크롤링된 이벤트 (실제 수집 시간 기준)
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    const recentlyCrawled = filteredData.filter(event => {
        const crawlDate = new Date(event.crawled_at || 0);
        return crawlDate >= thirtyDaysAgo;
    });

    // 지역 클러스터링 (영향 받은 지역 수 추정)
    const clusters = [];
    const CLUSTER_RADIUS = 1.0; // 약 111km

    filteredData.forEach(event => {
        let addedToCluster = false;
        
        for (let cluster of clusters) {
            const centerLat = cluster.events.reduce((sum, e) => sum + e.latitude, 0) / cluster.events.length;
            const centerLng = cluster.events.reduce((sum, e) => sum + e.longitude, 0) / cluster.events.length;
            
            const distance = Math.sqrt(
                Math.pow(event.latitude - centerLat, 2) + 
                Math.pow(event.longitude - centerLng, 2)
            );
            
            if (distance <= CLUSTER_RADIUS) {
                cluster.events.push(event);
                addedToCluster = true;
                break;
            }
        }
        
        if (!addedToCluster) {
            clusters.push({
                events: [event]
            });
        }
    });

    // Count events by data source
    const sourceCounts = {};
    filteredData.forEach(event => {
        const dataSource = event.data_source || getDataSourceFromEventId(event.event_id);
        sourceCounts[dataSource] = (sourceCounts[dataSource] || 0) + 1;
    });

    const sourceStatsHtml = Object.entries(sourceCounts).map(([source, count]) => {
        const displayName = {
            'rsoe': 'RSOE',
            'reliefweb': 'ReliefWeb',
            'emsc': 'EMSC'
        }[source] || source.toUpperCase();

        return `
            <div class="stat-box source-stat">
                <div class="stat-number source-${source}">${count.toLocaleString()}</div>
                <div class="stat-label">${displayName}</div>
            </div>
        `;
    }).join('');

    stats.innerHTML = `
        <div class="stat-box">
            <div class="stat-number">${totalEvents.toLocaleString()}</div>
            <div class="stat-label">총 이벤트</div>
        </div>
        <div class="stat-box">
            <div class="stat-number">${categories.length}</div>
            <div class="stat-label">카테고리</div>
        </div>
        <div class="stat-box">
            <div class="stat-number">${recentEvents.length}</div>
            <div class="stat-label">최근 7일</div>
        </div>
        <div class="stat-box">
            <div class="stat-number">${clusters.length}</div>
            <div class="stat-label">영향 지역</div>
        </div>
        ${sourceStatsHtml}
    `;
    
    // 콘솔에 상세 통계 출력
    if (totalEvents > 0) {
        console.log('📊 Current View Statistics:');
        console.log(`  Displayed events: ${totalEvents.toLocaleString()}`);
        console.log(`  Recent events (7 days): ${recentEvents.length}`);
        console.log(`  Recently crawled (30 days): ${recentlyCrawled.length}`);
        console.log(`  Geographic clusters: ${clusters.length}`);
    }
}

// 위험 지역 감지 (개선된 버전)
function checkRiskAlerts() {
    const riskAlert = document.getElementById('riskAlert');
    const riskMessage = document.getElementById('riskMessage');
    
    clearRiskAnimations();
    
    // 최근 30일 데이터로 위험도 분석
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    
    const recentEvents = filteredData.filter(event => {
        return event.event_date >= thirtyDaysAgo;
    });

    if (recentEvents.length === 0) {
        if (riskAlert) riskAlert.classList.add('hidden');
        return;
    }

    console.log(`🔍 Analyzing ${recentEvents.length} recent events for risk assessment`);

    // 지역별 클러스터링
    const clusters = [];
    const CLUSTER_RADIUS = 0.5; // 약 55km

    recentEvents.forEach(event => {
        let addedToCluster = false;
        
        for (let cluster of clusters) {
            const centerLat = cluster.events.reduce((sum, e) => sum + e.latitude, 0) / cluster.events.length;
            const centerLng = cluster.events.reduce((sum, e) => sum + e.longitude, 0) / cluster.events.length;
            
            const distance = Math.sqrt(
                Math.pow(event.latitude - centerLat, 2) + 
                Math.pow(event.longitude - centerLng, 2)
            );
            
            if (distance <= CLUSTER_RADIUS) {
                cluster.events.push(event);
                addedToCluster = true;
                break;
            }
        }
        
        if (!addedToCluster) {
            clusters.push({
                events: [event],
                centerLat: event.latitude,
                centerLng: event.longitude
            });
        }
    });

    // 고위험 지역 (30개 이상의 이벤트가 밀집된 지역, 최근 7일 내 5개 이상)
    const now = new Date();
    const riskClusters = clusters.filter(cluster => {
        const recentWeek = cluster.events.filter(event => {
            const eventDate = new Date(event.event_date_utc || event.crawled_at);
            const daysDiff = (now - eventDate) / (1000 * 60 * 60 * 24);
            return daysDiff <= 7;
        });
        
        // 매우 엄격한 기준: 전체 30개 이상 + 최근 7일 내 5개 이상
        return cluster.events.length >= 30 && recentWeek.length >= 5;
    });

    if (riskClusters.length > 0 && riskAlert && riskMessage) {
        const mostRiskyCluster = riskClusters.reduce((max, current) => 
            current.events.length > max.events.length ? current : max
        );
        
        const eventCount = mostRiskyCluster.events.length;
        const categories = [...new Set(mostRiskyCluster.events.map(e => e.event_category))];
        const centerLat = mostRiskyCluster.events.reduce((sum, e) => sum + e.latitude, 0) / mostRiskyCluster.events.length;
        const centerLng = mostRiskyCluster.events.reduce((sum, e) => sum + e.longitude, 0) / mostRiskyCluster.events.length;
        
        console.log(`⚠️ High risk area detected: ${eventCount} events at ${centerLat.toFixed(2)}, ${centerLng.toFixed(2)}`);
        
        riskMessage.innerHTML = `
            <strong>⚠️ 고위험 지역 감지!</strong><br>
            좌표 ${centerLat.toFixed(2)}, ${centerLng.toFixed(2)} 반경 55km 내에서<br>
            30일간 ${eventCount}건의 사건 발생 (${categories.join(', ')}).<br>
            해당 지역에 대한 주의가 필요합니다.
        `;
        riskAlert.classList.remove('hidden');
        showRiskAnimation(centerLat, centerLng, eventCount);
        
        // 다른 위험 지역들도 표시
        riskClusters.forEach((cluster, index) => {
            if (index > 0) { // 첫 번째는 이미 처리함
                const clusterLat = cluster.events.reduce((sum, e) => sum + e.latitude, 0) / cluster.events.length;
                const clusterLng = cluster.events.reduce((sum, e) => sum + e.longitude, 0) / cluster.events.length;
                showRiskAnimation(clusterLat, clusterLng, cluster.events.length);
            }
        });
        
    } else if (riskAlert) {
        riskAlert.classList.add('hidden');
    }
}

// 나머지 함수들은 기존과 동일... (길어서 생략)
// (기존 script.js의 나머지 함수들을 그대로 유지)

// 테이블 정렬 함수
function sortTable(column) {
    let direction = 'asc';
    if (currentSort.column === column) {
        if (currentSort.direction === 'asc') {
            direction = 'desc';
        } else if (currentSort.direction === 'desc') {
            direction = null;
        } else {
            direction = 'asc';
        }
    }

    document.querySelectorAll('th').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc', 'sort-active');
    });

    if (direction === null) {
        currentSort = { column: null, direction: null };
        sortedData = [...filteredData];
        populateTable(sortedData);
        return;
    }

    currentSort = { column, direction };

    const headerElement = document.querySelector(`th[data-column="${column}"]`);
    if (headerElement) {
        headerElement.classList.add('sort-active');
        headerElement.classList.add(direction === 'asc' ? 'sort-asc' : 'sort-desc');
    }

    sortedData = [...filteredData].sort((a, b) => {
        let aValue = a[column];
        let bValue = b[column];

        if (column === 'event_date') {
            aValue = a.event_date;
            bValue = b.event_date;
        } else if (column === 'latitude' || column === 'longitude') {
            aValue = parseFloat(aValue) || 0;
            bValue = parseFloat(bValue) || 0;
        } else if (column === 'event_id') {
            const aNum = parseInt(aValue.replace(/\D/g, '')) || 0;
            const bNum = parseInt(bValue.replace(/\D/g, '')) || 0;
            aValue = aNum;
            bValue = bNum;
        } else {
            aValue = (aValue || '').toString().toLowerCase();
            bValue = (bValue || '').toString().toLowerCase();
        }

        if (aValue === null || aValue === undefined) aValue = '';
        if (bValue === null || bValue === undefined) bValue = '';

        let comparison = 0;
        if (aValue > bValue) {
            comparison = 1;
        } else if (aValue < bValue) {
            comparison = -1;
        }

        return direction === 'desc' ? -comparison : comparison;
    });

    populateTable(sortedData);
}

function initializeData() {
    filteredData = [...disasterEvents];
    sortedData = [...filteredData];
    populateFilters();
    populateTable(sortedData);
    populateEventList(filteredData);
    updateStats();
    applyCurrentFilters();
    checkRiskAlerts();
    if (map) updateMapMarkers();
}

function initMap() {
    // Leaflet 가 로드되지 않았을 때에도 앱이 중단되지 않도록 가드
    if (typeof L === 'undefined') {
        console.error('Leaflet library is not loaded. Map features are disabled.');
        const mapEl = document.getElementById('map');
        if (mapEl) {
            mapEl.innerHTML = '<div style="padding:16px;color:#666;">지도 라이브러리를 불러오지 못했습니다. 네트워크 정책 또는 CDN 차단 여부를 확인해주세요.</div>';
        }
        return;
    }

    map = L.map('map').setView([20, 0], 2);

    const streetLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
        maxZoom: 18
    });

    const satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles &copy; Esri',
        maxZoom: 18
    });

    streetLayer.addTo(map);
    L.control.layers({
        'Street Map': streetLayer,
        'Satellite': satelliteLayer
    }, {}, { collapsed: false }).addTo(map);

    // MarkerCluster가 없으면 일반 레이어로 대체
    markersLayer = (L.markerClusterGroup && typeof L.markerClusterGroup === 'function')
        ? L.markerClusterGroup()
        : L.layerGroup();
    map.addLayer(markersLayer);
    updateMapMarkers();
}

function openEventUrl(url) {
    if (url && url !== '-' && url !== '') {
        window.open(url, '_blank');
    } else {
        alert('상세 정보 URL이 없습니다.');
    }
}

function updateMapMarkers() {
    if (!markersLayer) return;
    
    markersLayer.clearLayers();
    highlightedMarker = null;
    
    const markers = [];
    filteredData.forEach((event, index) => {
        // Skip events without valid coordinates
        if (!event.hasValidCoords) {
            return;
        }

        const color = getCategoryColor(event.event_category);
        const marker = L.circleMarker([event.latitude, event.longitude], {
            radius: 8,
            fillColor: color,
            color: '#fff',
            weight: 2,
            opacity: 1,
            fillOpacity: 0.8
        });

        const popupContent = `
            <div class="custom-popup">
                <div class="popup-title">${event.event_title}</div>
                <div class="popup-info">🆔 ${event.event_id}</div>
                <div class="popup-info">📍 ${event.latitude.toFixed(4)}, ${event.longitude.toFixed(4)}</div>
                <div class="popup-category category-${event.event_category.toLowerCase()}">${event.event_category}</div>
                <div class="popup-date">📅 ${event.event_date.toLocaleString()}</div>
                <div style="margin-top: 10px;">
                    <button onclick="openEventUrl('${event.event_url}')" style="background: #e74c3c; color: white; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer;">상세보기</button>
                </div>
            </div>
        `;
        marker.bindPopup(popupContent);
        marker._event_id = event.event_id;
        markers.push(marker);
    });

    if (typeof markersLayer.addLayers === 'function') {
        markersLayer.addLayers(markers);
    } else {
        markers.forEach(m => markersLayer.addLayer(m));
    }

    if (filteredData.length > 0 && markersLayer.getLayers().length > 0) {
        try {
            map.fitBounds(markersLayer.getBounds(), {padding: [20, 20]});
        } catch (e) {
            map.setView([20, 0], 2);
        }
    }
}

function getCategoryColor(category) {
    const colors = {
        'Earthquake': '#8b4513',
        'Volcanic eruption': '#ff6b35',
        'Surroundings explosion': '#d63447',
        'Industrial explosion': '#d63447',
        'Flood': '#1e3799',
        'Fire in built environment': '#ff3838',
        'Fire': '#ff3838',
        'War': '#8b0000',
        'Environment pollution': '#2d5016',
        'Landslide': '#8b4513'
    };
    return colors[category] || '#57606f';
}

function focusOnEvent(index) {
    const event = currentView === 'table' ? sortedData[index] : filteredData[index];
    if (event && map) {
        switchView('map');
        
        if (highlightedMarker) {
            highlightedMarker.setStyle({
                radius: 8,
                weight: 2,
                color: '#fff'
            });
        }
        
        const targetMarker = markersLayer.getLayers().find(m => m._event_id === event.event_id);

        if (targetMarker) {
            markersLayer.zoomToShowLayer(targetMarker, () => {
                targetMarker.setStyle({
                    radius: 15,
                    weight: 4,
                    color: '#ff0000'
                });
                targetMarker.openPopup();
                highlightedMarker = targetMarker;

                setTimeout(() => {
                    if (highlightedMarker === targetMarker) {
                        targetMarker.setStyle({
                            radius: 8,
                            weight: 2,
                            color: '#fff'
                        });
                        highlightedMarker = null;
                    }
                }, 3000);
            });
        } else {
             map.setView([event.latitude, event.longitude], 10);
        }
    }
}

function filterByDateRange(days) {
    activeDateRange = days;

    document.querySelectorAll('.date-range-btn').forEach(btn => {
        btn.classList.remove('active');
    });

    const targetBtn = document.querySelector(`.date-range-btn[onclick="filterByDateRange(${days})"]`);
    if (targetBtn) {
        targetBtn.classList.add('active');
    }

    applyCurrentFilters();
}

function applyTimeSlider() {
    const slider = document.getElementById('timeSlider');
    const sliderValue = document.getElementById('sliderValue');
    const timeRange = document.getElementById('timeRange');
    const currentEvents = document.getElementById('currentEvents');

    if (disasterEvents.length === 0) {
        timeRange.textContent = '데이터 없음';
        currentEvents.textContent = '0개';
        return;
    }

    const sliderValuePercent = parseInt(slider.value);
    const totalEvents = disasterEvents.length;
    const eventsToShow = Math.ceil((sliderValuePercent / 100) * totalEvents);
    
    sliderValue.textContent = `${sliderValuePercent}%`;
    
    if (sliderValuePercent === 100) {
        timeRange.textContent = '전체 기간';
        filteredData = [...disasterEvents];
    } else if (eventsToShow > 0) {
        const sortedEvents = [...disasterEvents].sort((a, b) => a.event_date - b.event_date);
        const cutoffDate = sortedEvents[eventsToShow - 1]?.event_date;
        const startDate = sortedEvents[0]?.event_date;
        
        if (cutoffDate && startDate) {
            timeRange.textContent = `${startDate.toLocaleDateString()} ~ ${cutoffDate.toLocaleDateString()}`;
            filteredData = disasterEvents.filter(event => event.event_date <= cutoffDate);
        } else {
            timeRange.textContent = '데이터 없음';
            filteredData = [];
        }
    } else {
        timeRange.textContent = '데이터 없음';
        filteredData = [];
    }
    
    currentEvents.textContent = `${filteredData.length}개`;
}

function applyCurrentFilters() {
    applyTimeSlider();

    const categoryFilter = document.getElementById('categoryFilter').value;
    const sourceFilter = document.getElementById('sourceFilter').value;
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const searchInput = document.getElementById('searchInput').value.toLowerCase();

    let tempFiltered = [...filteredData];

    if (activeDateRange !== null) {
        const now = new Date();
        const fromDate = new Date(now.setDate(now.getDate() - activeDateRange));
        tempFiltered = tempFiltered.filter(event => event.event_date >= fromDate);
    }

    if (categoryFilter) {
        tempFiltered = tempFiltered.filter(event => event.event_category === categoryFilter);
    }

    if (sourceFilter) {
        console.log(`🔍 Filtering by source: '${sourceFilter}'`);
        console.log(`📊 Before filter: ${tempFiltered.length} events`);
        tempFiltered = tempFiltered.filter(event => {
            const dataSource = event.data_source || getDataSourceFromEventId(event.event_id);
            const matches = dataSource === sourceFilter;
            if (sourceFilter === 'reliefweb' && dataSource === 'reliefweb') {
                console.log(`✅ ReliefWeb event matched: ${event.event_id} - ${event.event_title}`);
            }
            return matches;
        });
        console.log(`📊 After filter: ${tempFiltered.length} events`);
    }

    if (startDate) {
        const start = new Date(startDate);
        tempFiltered = tempFiltered.filter(event => event.event_date >= start);
    }

    if (endDate) {
        const end = new Date(endDate);
        end.setHours(23, 59, 59, 999);
        tempFiltered = tempFiltered.filter(event => event.event_date <= end);
    }

    if (searchInput) {
        tempFiltered = tempFiltered.filter(event =>
            event.event_title.toLowerCase().includes(searchInput)
        );
    }

    filteredData = tempFiltered;
    
    if (currentSort.column && currentSort.direction) {
        const column = currentSort.column;
        const direction = currentSort.direction;
        
        sortedData = [...filteredData].sort((a, b) => {
            let aValue = a[column];
            let bValue = b[column];

            if (column === 'event_date') {
                aValue = a.event_date;
                bValue = b.event_date;
            } else if (column === 'latitude' || column === 'longitude') {
                aValue = parseFloat(aValue) || 0;
                bValue = parseFloat(bValue) || 0;
            } else if (column === 'event_id') {
                const aNum = parseInt(aValue.replace(/\D/g, '')) || 0;
                const bNum = parseInt(bValue.replace(/\D/g, '')) || 0;
                aValue = aNum;
                bValue = bNum;
            } else {
                aValue = (aValue || '').toString().toLowerCase();
                bValue = (bValue || '').toString().toLowerCase();
            }

            if (aValue === null || aValue === undefined) aValue = '';
            if (bValue === null || bValue === undefined) bValue = '';

            let comparison = 0;
            if (aValue > bValue) {
                comparison = 1;
            } else if (aValue < bValue) {
                comparison = -1;
            }

            return direction === 'desc' ? -comparison : comparison;
        });
    } else {
        sortedData = [...filteredData];
    }

    tableItemsToShow = 50; // Reset for infinite scroll
    populateTable(sortedData);
    populateEventList(filteredData);
    updateStats();
    if (map) updateMapMarkers();
    checkRiskAlerts();
}

function toggleAnimation() {
    const playBtn = document.getElementById('playBtn');
    
    if (isPlaying) {
        clearInterval(animationInterval);
        isPlaying = false;
        playBtn.textContent = '▶ 재생';
        playBtn.classList.remove('playing');
    } else {
        startAnimation();
        isPlaying = true;
        playBtn.textContent = '⏸ 정지';
        playBtn.classList.add('playing');
    }
}

function startAnimation() {
    const slider = document.getElementById('timeSlider');
    currentTimeIndex = 0;
    
    animationInterval = setInterval(() => {
        currentTimeIndex += 5;
        if (currentTimeIndex > 100) {
            clearInterval(animationInterval);
            isPlaying = false;
            const playBtn = document.getElementById('playBtn');
            playBtn.textContent = '▶ 재생';
            playBtn.classList.remove('playing');
            return;
        }
        
        slider.value = currentTimeIndex;
        applyCurrentFilters();
    }, animationSpeed);
}

function setAnimationSpeed(speed) {
    animationSpeed = speed;
    
    document.querySelectorAll('.speed-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    document.querySelector(`[data-speed="${speed}"]`).classList.add('active');
    
    if (isPlaying) {
        clearInterval(animationInterval);
        startAnimation();
    }
}

function showRiskAnimation(lat, lng, eventCount) {
    if (!map) return;
    
    const riskMarker = L.marker([lat, lng], {
        icon: L.divIcon({
            className: 'risk-marker',
            html: `
                <div style="position: relative; width: 40px; height: 40px;">
                    <div class="risk-ripple" style="width: 40px; height: 40px; left: 0; top: 0;"></div>
                    <div class="risk-ripple" style="width: 40px; height: 40px; left: 0; top: 0;"></div>
                    <div class="risk-ripple" style="width: 40px; height: 40px; left: 0; top: 0;"></div>
                    <div style="position: absolute; left: 50%; top: 50%; transform: translate(-50%, -50%); 
                                background: #ff4757; color: white; border-radius: 50%; 
                                width: 24px; height: 24px; display: flex; align-items: center; 
                                justify-content: center; font-weight: bold; font-size: 12px; z-index: 1001;">
                        ⚠️
                    </div>
                </div>
            `,
            iconSize: [40, 40],
            iconAnchor: [20, 20]
        })
    });
    
    const popupContent = `
        <div class="custom-popup">
            <div class="popup-title" style="color: #ff4757;">🚨 고위험 지역</div>
            <div class="popup-info">📍 ${lat.toFixed(4)}, ${lng.toFixed(4)}</div>
            <div class="popup-info">📊 ${eventCount}건의 사건 발생</div>
            <div class="popup-info">⚠️ 주의 필요 지역</div>
        </div>
    `;
    riskMarker.bindPopup(popupContent);
    riskMarker.addTo(map);
    riskAnimationMarkers.push(riskMarker);
}

function clearRiskAnimations() {
    riskAnimationMarkers.forEach(marker => {
        if (map.hasLayer(marker)) {
            map.removeLayer(marker);
        }
    });
    riskAnimationMarkers = [];
}

function populateFilters() {
    const categories = [...new Set(disasterEvents.map(event => event.event_category))].sort();
    const categoryFilter = document.getElementById('categoryFilter');
    
    if (categoryFilter) {
        while (categoryFilter.children.length > 1) {
            categoryFilter.removeChild(categoryFilter.lastChild);
        }
        
        categories.forEach(category => {
            const option = document.createElement('option');
            option.value = category;
            option.textContent = category;
            categoryFilter.appendChild(option);
        });
    }

    if (disasterEvents.length > 0) {
        const dates = disasterEvents.map(event => event.event_date);
        const minDate = new Date(Math.min(...dates));
        const maxDate = new Date(Math.max(...dates));
        
        const startDateInput = document.getElementById('startDate');
        const endDateInput = document.getElementById('endDate');
        
        if (startDateInput) startDateInput.value = minDate.toISOString().split('T')[0];
        if (endDateInput) endDateInput.value = maxDate.toISOString().split('T')[0];
    }
}

function populateTable(data, append = false) {
    const tbody = document.getElementById('tableBody');
    if (!tbody) return;

    if (!append) {
        tbody.innerHTML = '';
    }

    const start = append ? tbody.rows.length : 0;
    const end = start + tableItemsToShow;
    const newData = data.slice(start, end);

    newData.forEach((event, index) => {
        const row = tbody.insertRow();
        row.insertCell(0).textContent = event.event_id;
        row.insertCell(1).textContent = event.event_title;

        const categoryCell = row.insertCell(2);
        categoryCell.innerHTML = `<span class="category-${event.event_category.toLowerCase().replace(/\s+/g, '-')}">${event.event_category}</span>`;

        // Data source cell
        const sourceCell = row.insertCell(3);
        const dataSource = event.data_source || getDataSourceFromEventId(event.event_id);
        const sourceDisplay = {
            'rsoe': 'RSOE',
            'reliefweb': 'ReliefWeb',
            'emsc': 'EMSC'
        }[dataSource] || 'RSOE';
        sourceCell.innerHTML = `<span class="source-${dataSource}">${sourceDisplay}</span>`;

        row.insertCell(4).textContent = event.event_date.toLocaleString();
        row.insertCell(5).textContent = event.address || '위치 정보 없음';
        row.insertCell(6).textContent = event.latitude.toFixed(6);
        row.insertCell(7).textContent = event.longitude.toFixed(6);

        const detailCell = row.insertCell(8);
        detailCell.innerHTML = `<button onclick="openEventUrl('${event.event_url}')" style="background: #e74c3c; color: white; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer;">상세보기</button>`;

        row.onclick = () => focusOnEvent(start + index);
        row.style.cursor = 'pointer';
    });
}

function populateEventList(data) {
    const eventList = document.getElementById('eventList');
    if (!eventList) return;
    
    eventList.innerHTML = '';
    
    data.forEach((event, index) => {
        const eventItem = document.createElement('div');
        eventItem.className = 'event-item';
        eventItem.onclick = () => focusOnEvent(index);
        
        eventItem.innerHTML = `
            <div class=\"event-title">${event.event_title}</div>
            <div class=\"event-meta">
                <span class=\"category-${event.event_category.toLowerCase().replace(/\s+/g, '-')}">${event.event_category}</span>
                <span>${event.event_date.toLocaleString()}</span>
            </div>
            <div style=\"font-size: 12px; color: #666; margin-top: 5px;">
                📍 ${event.address || '위치 정보 없음'}
            </div>
        `;
        
        eventList.appendChild(eventItem);
    });
}

function switchView(view) {
    const mapContainer = document.getElementById('mapContainer');
    const tableContainer = document.getElementById('tableContainer');
    const listContainer = document.getElementById('listContainer');
    const mapViewBtn = document.getElementById('mapView');
    const tableViewBtn = document.getElementById('tableView');
    const listViewBtn = document.getElementById('listView');
    
    if (mapContainer) mapContainer.classList.add('hidden');
    if (tableContainer) tableContainer.classList.add('hidden');
    if (listContainer) listContainer.classList.add('hidden');
    
    if (mapViewBtn) mapViewBtn.classList.remove('active');
    if (tableViewBtn) tableViewBtn.classList.remove('active');
    if (listViewBtn) listViewBtn.classList.remove('active');
    
    currentView = view;
    
    if (view === 'map') {
        if (mapContainer) mapContainer.classList.remove('hidden');
        if (mapViewBtn) mapViewBtn.classList.add('active');
        setTimeout(() => {
            if (map) map.invalidateSize();
        }, 100);
    } else if (view === 'table') {
        if (tableContainer) tableContainer.classList.remove('hidden');
        if (tableViewBtn) tableViewBtn.classList.add('active');
    } else if (view === 'list') {
        if (listContainer) listContainer.classList.remove('hidden');
        if (listViewBtn) listViewBtn.classList.add('active');
    }
}

function filterData() {
    applyCurrentFilters();
}

function getDataSourceFromEventId(eventId) {
    // Determine data source from event ID prefix
    if (eventId && typeof eventId === 'string') {
        if (eventId.startsWith('RW_')) {
            return 'reliefweb';
        } else if (eventId.startsWith('EMSC_')) {
            return 'emsc';
        }
    }
    return 'rsoe'; // Default to RSOE for legacy events
}

function resetFilters() {
    activeDateRange = null;
    document.querySelectorAll('.date-range-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector('.date-range-btn[onclick="filterByDateRange(null)"]').classList.add('active');

    const categoryFilter = document.getElementById('categoryFilter');
    const sourceFilter = document.getElementById('sourceFilter');
    const startDate = document.getElementById('startDate');
    const endDate = document.getElementById('endDate');
    const searchInput = document.getElementById('searchInput');
    const timeSlider = document.getElementById('timeSlider');

    if (categoryFilter) categoryFilter.value = '';
    if (sourceFilter) sourceFilter.value = '';
    if (startDate) startDate.value = '';
    if (endDate) endDate.value = '';
    if (searchInput) searchInput.value = '';
    if (timeSlider) timeSlider.value = 100;
    
    // 정렬 초기화
    currentSort = { column: null, direction: null };
    document.querySelectorAll('th').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc', 'sort-active');
    });
    
    filterData();
}

function downloadExcel() {
    if (typeof XLSX === 'undefined') {
        alert('Excel 다운로드 기능을 사용할 수 없습니다. 페이지를 새로고침해주세요.');
        return;
    }
    
    const dataToExport = currentView === 'table' ? sortedData : filteredData;
    const data = dataToExport.map(event => ({
        'Event ID': event.event_id,
        'Title': event.event_title,
        'Category': event.event_category,
        'Data Source': event.data_source || getDataSourceFromEventId(event.event_id),
        'Date': event.event_date_utc,
        'Latitude': event.latitude,
        'Longitude': event.longitude,
        'Address': event.address,
        'Source': event.source,
        'URL': event.event_url
    }));
    
    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Disaster Events");
    XLSX.writeFile(wb, `disaster_events_${new Date().toISOString().split('T')[0]}.xlsx`);
}

function downloadCSV() {
    const dataToExport = currentView === 'table' ? sortedData : filteredData;
    const csvContent = [
        ['Event ID', 'Title', 'Category', 'Data Source', 'Date', 'Latitude', 'Longitude', 'Address', 'Source', 'URL'],
        ...dataToExport.map(event => [
            event.event_id,
            event.event_title,
            event.event_category,
            event.data_source || getDataSourceFromEventId(event.event_id),
            event.event_date_utc,
            event.latitude,
            event.longitude,
            event.address || '',
            event.source || '',
            event.event_url
        ])
    ].map(row => row.map(field => `"${field}"`).join(',')).join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', `disaster_events_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// 이벤트 리스너 설정
function setupEventListeners() {
    const timeSlider = document.getElementById('timeSlider');
    if (timeSlider) {
        timeSlider.addEventListener('input', applyCurrentFilters);
    }

    const tableContainer = document.querySelector('.table-container');
    if (tableContainer) {
        tableContainer.addEventListener('scroll', () => {
            if (tableContainer.scrollTop + tableContainer.clientHeight >= tableContainer.scrollHeight - 5) {
                populateTable(sortedData, true);
            }
        });
    }

    // 보기 전환 버튼 (CSP 환경에서 inline onclick 방지)
    const mapViewBtn = document.getElementById('mapView');
    const tableViewBtn = document.getElementById('tableView');
    const listViewBtn = document.getElementById('listView');
    if (mapViewBtn) mapViewBtn.addEventListener('click', () => switchView('map'));
    if (tableViewBtn) tableViewBtn.addEventListener('click', () => switchView('table'));
    if (listViewBtn) listViewBtn.addEventListener('click', () => switchView('list'));

    // 다운로드 버튼
    const excelBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('Excel'));
    const csvBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('CSV'));
    if (excelBtn) excelBtn.addEventListener('click', downloadExcel);
    if (csvBtn) csvBtn.addEventListener('click', downloadCSV);

    // 필터 초기화
    const resetBtn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.includes('필터 초기화'));
    if (resetBtn) resetBtn.addEventListener('click', resetFilters);

    // 새로고침 버튼
    const refreshBtn = document.getElementById('refreshBtn');
    if (refreshBtn) refreshBtn.addEventListener('click', refreshData);

    // 날짜 범위 버튼들
    const rangeButtons = document.querySelectorAll('.date-range-btn');
    if (rangeButtons && rangeButtons.length) {
        const ranges = [null, 1, 3, 7]; // 버튼 순서: 전체, 24시간, 3일, 7일
        rangeButtons.forEach((btn, idx) => {
            btn.addEventListener('click', () => filterByDateRange(ranges[idx]));
        });
    }
    
    // 키보드 단축키
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && isPlaying) {
            toggleAnimation();
        }
        if (e.key === ' ' && e.target.tagName !== 'INPUT' && e.target.tagName !== 'TEXTAREA') {
            e.preventDefault();
            toggleAnimation();
        }
    });
}

// 자동 새로고침 (10분마다)
function startAutoRefresh() {
    setInterval(() => {
        console.log('🔄 Auto-refreshing data...');
        loadDisasterData();
    }, 10 * 60 * 1000); // 10분
}

// 페이지 로드 시 초기화
window.addEventListener('load', function() {
    console.log('🌍 Initializing disaster monitoring system...');
    
    // 데이터 로드
    loadDisasterData();
    
    // 지도 초기화
    try {
        initMap();
    } catch (e) {
        console.error('Map initialization failed:', e);
    }
    
    // 이벤트 리스너 설정
    setupEventListeners();
    
    // 자동 새로고침 시작 (10분마다)
    startAutoRefresh();
    
    console.log('✅ System initialized successfully!');
});

// 창 크기 변경 시 지도 크기 조정
window.addEventListener('resize', function() {
    if (map && currentView === 'map') {
        setTimeout(() => map.invalidateSize(), 100);
    }
});

// 페이지 가시성 변경 시 자동 새로고침
document.addEventListener('visibilitychange', function() {
    if (!document.hidden) {
        // 페이지가 다시 보일 때 데이터 새로고침
        console.log('👀 Page became visible, refreshing data...');
        loadDisasterData();
    }
});
