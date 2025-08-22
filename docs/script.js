// 전역 변수
let disasterEvents = [];
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

// 데이터 로딩 함수
async function loadDisasterData() {
    const loadingIndicator = document.getElementById('loadingIndicator');
    const errorMessage = document.getElementById('errorMessage');
    const refreshBtn = document.getElementById('refreshBtn');
    
    try {
        loadingIndicator.classList.remove('hidden');
        errorMessage.classList.add('hidden');
        refreshBtn.disabled = true;
        refreshBtn.textContent = '로딩 중...';
        
        // JSON 데이터 로드
        const response = await fetch('./data/events.json?t=' + new Date().getTime());
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // 업데이트 시간 로드
        try {
            const updateResponse = await fetch('./data/last_update.txt?t=' + new Date().getTime());
            if (updateResponse.ok) {
                const updateText = await updateResponse.text();
                lastUpdateTime = updateText.replace('Last updated: ', '').trim();
            }
        } catch (e) {
            console.log('Could not load update time:', e);
        }
        
        // 데이터 전처리
        disasterEvents = data.map(event => ({
            ...event,
            latitude: parseFloat(event.latitude),
            longitude: parseFloat(event.longitude),
            event_date: new Date(event.event_date_utc)
        })).filter(event => !isNaN(event.latitude) && !isNaN(event.longitude));

        // 시간순 정렬
        disasterEvents.sort((a, b) => a.event_date - b.event_date);
        
        console.log(`Loaded ${disasterEvents.length} events`);
        loadingIndicator.classList.add('hidden');
        
        initializeData();
        updateLastUpdateDisplay();
        
    } catch (error) {
        console.error('Failed to load disaster data:', error);
        loadingIndicator.classList.add('hidden');
        errorMessage.classList.remove('hidden');
    } finally {
        refreshBtn.disabled = false;
        refreshBtn.textContent = '🔄 새로고침';
        refreshBtn.classList.remove('refreshing');
    }
}

// 마지막 업데이트 시간 표시
function updateLastUpdateDisplay() {
    const lastUpdateElement = document.getElementById('lastUpdate');
    if (lastUpdateTime) {
        lastUpdateElement.textContent = `최종 업데이트: ${lastUpdateTime}`;
    } else if (disasterEvents.length > 0) {
        const latestEvent = disasterEvents.reduce((latest, event) => 
            new Date(event.crawled_at) > new Date(latest.crawled_at) ? event : latest
        );
        lastUpdateElement.textContent = `최종 업데이트: ${new Date(latestEvent.crawled_at).toLocaleString()}`;
    } else {
        lastUpdateElement.textContent = '데이터 없음';
    }
}

// 데이터 새로고침
async function refreshData() {
    const refreshBtn = document.getElementById('refreshBtn');
    refreshBtn.classList.add('refreshing');
    refreshBtn.textContent = '새로고침 중...';
    
    await loadDisasterData();
}

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
    updateTimeSlider();
    checkRiskAlerts();
    if (map) updateMapMarkers();
}

function initMap() {
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

    markersLayer = L.layerGroup().addTo(map);
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
    
    filteredData.forEach((event, index) => {
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
        markersLayer.addLayer(marker);
    });

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
        map.setView([event.latitude, event.longitude], 10);
        switchView('map');
        
        if (highlightedMarker) {
            highlightedMarker.setStyle({
                radius: 8,
                weight: 2,
                color: '#fff'
            });
        }
        
        const markers = markersLayer.getLayers();
        const targetIndex = filteredData.findIndex(e => e.event_id === event.event_id);
        const targetMarker = markers[targetIndex];
        if (targetMarker) {
            targetMarker.setStyle({
                radius: 15,
                weight: 4,
                color: '#ff0000'
            });
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
        }
    }
}

function updateTimeSlider() {
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
    applyCurrentFilters();
}

function applyCurrentFilters() {
    const categoryFilter = document.getElementById('categoryFilter').value;
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    const searchInput = document.getElementById('searchInput').value.toLowerCase();

    let tempFiltered = [...filteredData];

    if (categoryFilter) {
        tempFiltered = tempFiltered.filter(event => event.event_category === categoryFilter);
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
        updateTimeSlider();
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

function checkRiskAlerts() {
    const riskAlert = document.getElementById('riskAlert');
    const riskMessage = document.getElementById('riskMessage');
    
    clearRiskAnimations();
    
    const recentEvents = filteredData.filter(event => {
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
        return event.event_date >= thirtyDaysAgo;
    });

    if (recentEvents.length === 0) {
        if (riskAlert) riskAlert.classList.add('hidden');
        return;
    }

    const clusters = [];
    const CLUSTER_RADIUS = 0.5;

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

    const riskClusters = clusters.filter(cluster => cluster.events.length >= 5);

    if (riskClusters.length > 0 && riskAlert && riskMessage) {
        const mostRiskyCluster = riskClusters.reduce((max, current) => 
            current.events.length > max.events.length ? current : max
        );
        
        const eventCount = mostRiskyCluster.events.length;
        const categories = [...new Set(mostRiskyCluster.events.map(e => e.event_category))];
        const centerLat = mostRiskyCluster.events.reduce((sum, e) => sum + e.latitude, 0) / mostRiskyCluster.events.length;
        const centerLng = mostRiskyCluster.events.reduce((sum, e) => sum + e.longitude, 0) / mostRiskyCluster.events.length;
        
        riskMessage.innerHTML = `
            <strong>⚠️ 고위험 지역 감지!</strong><br>
            좌표 ${centerLat.toFixed(2)}, ${centerLng.toFixed(2)} 반경 55km 내에서<br>
            30일간 ${eventCount}건의 사건 발생 (${categories.join(', ')}).<br>
            해당 지역에 대한 주의가 필요합니다.
        `;
        riskAlert.classList.remove('hidden');
        showRiskAnimation(centerLat, centerLng, eventCount);
    } else if (riskAlert) {
        riskAlert.classList.add('hidden');
    }
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

function populateTable(data) {
    const tbody = document.getElementById('tableBody');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    
    data.forEach((event, index) => {
        const row = tbody.insertRow();
        row.insertCell(0).textContent = event.event_id;
        row.insertCell(1).textContent = event.event_title;
        
        const categoryCell = row.insertCell(2);
        categoryCell.innerHTML = `<span class="category-${event.event_category.toLowerCase().replace(/\s+/g, '-')}">${event.event_category}</span>`;
        
        row.insertCell(3).textContent = event.event_date.toLocaleString();
        row.insertCell(4).textContent = event.address || '위치 정보 없음';
        row.insertCell(5).textContent = event.latitude.toFixed(6);
        row.insertCell(6).textContent = event.longitude.toFixed(6);
        
        const detailCell = row.insertCell(7);
        detailCell.innerHTML = `<button onclick="openEventUrl('${event.event_url}')" style="background: #e74c3c; color: white; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer;">상세보기</button>`;
        
        row.onclick = () => focusOnEvent(index);
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
            <div class="event-title">${event.event_title}</div>
            <div class="event-meta">
                <span class="category-${event.event_category.toLowerCase().replace(/\s+/g, '-')}">${event.event_category}</span>
                <span>${event.event_date.toLocaleString()}</span>
            </div>
            <div style="font-size: 12px; color: #666; margin-top: 5px;">
                📍 ${event.address || '위치 정보 없음'}
            </div>
        `;
        
        eventList.appendChild(eventItem);
    });
}

function updateStats() {
    const stats = document.getElementById('stats');
    if (!stats) return;
    
    const totalEvents = filteredData.length;
    const categories = [...new Set(filteredData.map(event => event.event_category))];
    const recentEvents = filteredData.filter(event => {
        const sevenDaysAgo = new Date();
        sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
        return event.event_date >= sevenDaysAgo;
    });

    const clusters = [];
    const CLUSTER_RADIUS = 1.0;

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

    stats.innerHTML = `
        <div class="stat-box">
            <div class="stat-number">${totalEvents}</div>
            <div class="stat-label">이 이벤트</div>
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
    `;
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
    updateTimeSlider();
}

function resetFilters() {
    const categoryFilter = document.getElementById('categoryFilter');
    const startDate = document.getElementById('startDate');
    const endDate = document.getElementById('endDate');
    const searchInput = document.getElementById('searchInput');
    const timeSlider = document.getElementById('timeSlider');
    
    if (categoryFilter) categoryFilter.value = '';
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
        ['Event ID', 'Title', 'Category', 'Date', 'Latitude', 'Longitude', 'Address', 'Source', 'URL'],
        ...dataToExport.map(event => [
            event.event_id,
            event.event_title,
            event.event_category,
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
        timeSlider.addEventListener('input', updateTimeSlider);
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

// 자동 새로고침 (5분마다)
function startAutoRefresh() {
    setInterval(() => {
        console.log('Auto-refreshing data...');
        loadDisasterData();
    }, 5 * 60 * 1000); // 5분
}

// 페이지 로드 시 초기화
window.addEventListener('load', function() {
    console.log('Initializing disaster monitoring system...');
    
    // 데이터 로드
    loadDisasterData();
    
    // 지도 초기화
    initMap();
    
    // 이벤트 리스너 설정
    setupEventListeners();
    
    // 자동 새로고침 시작 (5분마다)
    startAutoRefresh();
    
    console.log('System initialized successfully!');
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
        console.log('Page became visible, refreshing data...');
        loadDisasterData();
    }
});