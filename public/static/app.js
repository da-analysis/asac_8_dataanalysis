// public/static/app.js
// FRAI - 프랜차이즈 브랜드 추천 서비스 (업데이트 버전)

// 지역 데이터 (시도-시군구 매핑)
const REGION_DATA = {
    '강원도': ['강릉시', '고성군', '동해시', '삼척시', '속초시', '양양군', '영월군', '원주시', '인제군', '정선군', '철원군', '춘천시', '태백시', '평창군', '홍천군', '화천군', '횡성군'],
    '경기도': ['고양시 덕양구', '고양시 일산동구', '고양시 일산서구', '과천시', '광명시', '광주시', '구리시', '군포시', '김포시', '남양주시', '동두천시', '부천시', '부천시 소사구', '부천시 오정구', '부천시 원미구', '성남시 분당구', '성남시 수정구', '성남시 중원구', '수원시 권선구', '수원시 영통구', '수원시 장안구', '수원시 팔달구', '시흥시', '안산시 단원구', '안산시 상록구', '안성시', '안양시 동안구', '안양시 만안구', '양주시', '양평군', '여주시', '연천군', '오산시', '용인시 기흥구', '용인시 수지구', '용인시 처인구', '의왕시', '의정부시', '이천시', '파주시', '평택시', '포천시', '하남시', '화성시'],
    '경상남도': ['거제시', '거창군', '고성군', '김해시', '남해군', '밀양시', '사천시', '산청군', '양산시', '진주시', '창녕군', '창원시 마산합포구', '창원시 마산회원구', '창원시 성산구', '창원시 의창구', '창원시 진해구', '통영시', '하동군', '함안군', '함양군', '합천군'],
    '경상북도': ['경산시', '경주시', '고령군', '구미시', '김천시', '문경시', '상주시', '성주군', '안동시', '영양군', '영주시', '영천시', '예천군', '청도군', '청송군', '칠곡군', '포항시 남구', '포항시 북구'],
    '광주광역시': ['광산구', '남구', '동구', '북구', '서구'],
    '대구광역시': ['남구', '달서구', '달성군', '동구', '북구', '서구', '수성구', '중구'],
    '대전광역시': ['대덕구', '동구', '서구', '유성구', '중구'],
    '부산광역시': ['강서구', '금정구', '기장군', '남구', '동구', '동래구', '부산진구', '북구', '사상구', '사하구', '서구', '수영구', '연제구', '영도구', '중구', '해운대구'],
    '서울특별시': ['강남구', '강동구', '강북구', '강서구', '관악구', '광진구', '구로구', '금천구', '노원구', '도봉구', '동대문구', '동작구', '마포구', '서대문구', '서초구', '성동구', '성북구', '송파구', '양천구', '영등포구', '용산구', '은평구', '종로구', '중구', '중랑구'],
    '세종특별자치시': ['세종특별자치시'],
    '울산광역시': ['남구', '동구', '북구', '울주군', '중구'],
    '인천광역시': ['강화군', '계양구', '남동구', '동구', '미추홀구', '부평구', '서구', '연수구', '중구'],
    '전라남도': ['강진군', '고흥군', '곡성군', '광양시', '구례군', '나주시', '담양군', '목포시', '무안군', '보성군', '순천시', '여수시', '영암군', '완도군', '장성군', '장흥군', '진도군', '함평군', '해남군', '화순군'],
    '전라북도': ['고창군', '군산시', '김제시', '남원시', '무주군', '부안군', '순창군', '완주군', '익산시', '임실군', '전주시 덕진구', '전주시 완산구', '정읍시', '진안군'],
    '제주특별자치도': ['서귀포시', '제주시'],
    '충청남도': ['계룡시', '공주시', '금산군', '논산시', '당진시', '보령시', '부여군', '서산시', '서천군', '아산시', '예산군', '천안시 동남구', '천안시 서북구', '청양군', '태안군', '홍성군'],
    '충청북도': ['괴산군', '단양군', '보은군', '영동군', '옥천군', '음성군', '제천시', '증평군', '진천군', '청주시 상당구', '청주시 서원구', '청주시 청원구', '청주시 흥덕구', '충주시']
};

// 지역명 변환 매핑 (표시명 → 내부 코드)
const REGION_CODE_MAP = {
    '강원도': '강원',
    '경기도': '경기',
    '경상남도': '경남',
    '경상북도': '경북',
    '광주광역시': '광주',
    '대구광역시': '대구',
    '대전광역시': '대전',
    '부산광역시': '부산',
    '서울특별시': '서울',
    '세종특별자치시': '세종',
    '울산광역시': '울산',
    '인천광역시': '인천',
    '전라남도': '전남',
    '전라북도': '전북',
    '제주특별자치도': '제주',
    '충청남도': '충남',
    '충청북도': '충북'
};

// 기타 업종 목록
const OTHER_INDUSTRIES = [
    '스포츠 관련', '교육 (외국어)', '기타도소매', '기타 서비스', '기타 교육', '이미용', 
    '자동차 관련', '교육 (교과)', '아이스크림/빙수', '안경', '숙박', '반려동물 관련', 
    '유아 관련 (교육 외)', '의류 / 패션', '운송', '농수산물', '오락', '세탁', '임대', 
    'PC방', '인력 파견', '(건강)식품', '종합소매점', '화장품', '유아관련', '배달', 
    '약국', '부동산 중개', '이사'
];

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 DOM Content Loaded - Starting App');
    
    const form = document.getElementById('recommendationForm');
    const loadingSection = document.getElementById('loadingSection');
    const resultSection = document.getElementById('resultSection');
    const errorSection = document.getElementById('errorSection');
    const submitBtn = document.getElementById('submitBtn');
    const submitText = document.getElementById('submitText');
    const sidoSelect = document.getElementById('sido');
    const sigunguSelect = document.getElementById('sigungu');
    const otherIndustryModal = document.getElementById('otherIndustryModal');

    console.log('📋 Elements Found:', {
        form: !!form,
        loadingSection: !!loadingSection,
        resultSection: !!resultSection,
        errorSection: !!errorSection,
        submitBtn: !!submitBtn
    });

    let selectedIndustry = ''; // 기본값 제거
    let selectedBudget = ''; // 기본값 제거

    // 시도 옵션 생성
    function populateSidoOptions() {
        Object.keys(REGION_DATA).forEach(sido => {
            const option = document.createElement('option');
            option.value = sido;
            option.textContent = sido;
            sidoSelect.appendChild(option);
        });
    }

    // 시군구 옵션 업데이트
    function updateSigunguOptions(sido) {
        sigunguSelect.innerHTML = '';
        sigunguSelect.disabled = false;
        
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = '시/군/구 선택';
        sigunguSelect.appendChild(defaultOption);

        if (REGION_DATA[sido]) {
            REGION_DATA[sido].forEach(sigungu => {
                const option = document.createElement('option');
                option.value = sigungu;
                option.textContent = sigungu;
                sigunguSelect.appendChild(option);
            });
        }
    }

    // 시도 변경 이벤트
    sidoSelect.addEventListener('change', function() {
        const selectedSido = this.value;
        if (selectedSido) {
            updateSigunguOptions(selectedSido);
        } else {
            sigunguSelect.innerHTML = '<option value="">시/도를 먼저 선택하세요</option>';
            sigunguSelect.disabled = true;
        }
    });

    // 기타 업종 모달 생성
    function createOtherIndustryModal() {
        const otherIndustryButtons = document.getElementById('otherIndustryButtons');
        otherIndustryButtons.innerHTML = '';
        
        OTHER_INDUSTRIES.forEach(industry => {
            const button = document.createElement('button');
            button.type = 'button';
            button.className = 'px-3 py-2 text-sm border border-gray-300 rounded hover:bg-gray-50 text-left';
            button.textContent = industry;
            button.onclick = () => selectOtherIndustry(industry);
            otherIndustryButtons.appendChild(button);
        });
    }

    // 기타 업종 선택
    function selectOtherIndustry(industry) {
        selectedIndustry = industry;
        
        // 모든 업종 버튼 선택 해제
        document.querySelectorAll('.industry-btn').forEach(btn => {
            btn.classList.remove('selected');
        });
        
        // 기타 버튼 선택 표시 및 텍스트 변경
        const otherBtn = document.querySelector('[data-value="기타"]');
        otherBtn.classList.add('selected');
        otherBtn.textContent = industry;
        otherBtn.dataset.value = industry;
        
        // 모달 닫기
        otherIndustryModal.classList.add('hidden');
    }

    // 업종 버튼 이벤트 처리
    const industryButtons = document.querySelectorAll('#industryButtons .industry-btn');
    industryButtons.forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            
            if (this.dataset.value === '기타') {
                // 기타 업종 모달 열기
                otherIndustryModal.classList.remove('hidden');
                return;
            }
            
            // 일반 업종 선택
            industryButtons.forEach(b => b.classList.remove('selected'));
            this.classList.add('selected');
            selectedIndustry = this.dataset.value;
            
            // 기타 버튼이 선택되어 있었다면 초기화
            const otherBtn = document.querySelector('[data-value="기타"]');
            if (otherBtn && !OTHER_INDUSTRIES.includes(otherBtn.dataset.value)) {
                otherBtn.textContent = '기타';
                otherBtn.dataset.value = '기타';
            }
        });
    });

    // 기타 업종 모달 취소
    document.getElementById('cancelOtherIndustry').addEventListener('click', function() {
        otherIndustryModal.classList.add('hidden');
    });

    // 모달 배경 클릭으로 닫기
    otherIndustryModal.addEventListener('click', function(e) {
        if (e.target === this) {
            this.classList.add('hidden');
        }
    });

    // 예산 버튼 이벤트 처리
    const budgetButtons = document.querySelectorAll('#budgetButtons .budget-btn');
    budgetButtons.forEach(btn => {
        btn.addEventListener('click', function(e) {
            e.preventDefault();
            budgetButtons.forEach(b => b.classList.remove('selected'));
            this.classList.add('selected');
            selectedBudget = this.dataset.value;
        });
    });

    // 폼 제출 이벤트 처리
    form.addEventListener('submit', async function(e) {
        e.preventDefault();
        await handleFormSubmit();
    });

    // 폼 제출 및 API 호출 처리
    async function handleFormSubmit() {
        console.log('🎯 Form Submit Started');
        try {
            // UI 초기화
            hideAllSections();
            showLoadingSection();
            
            // 폼 데이터 수집
            const formData = new FormData(form);
            const sido = formData.get('sido');
            const sigungu = formData.get('sigungu');
            const needs = formData.get('needs') || '';

            // 입력값 검증
            if (!sido || !sigungu) {
                showError('지역을 선택해주세요.');
                return;
            }
            
            if (!selectedIndustry) {
                showError('업종을 선택해주세요.');
                return;
            }
            
            if (!selectedBudget) {
                showError('예산을 선택해주세요.');
                return;
            }

            // 지역명 변환 (표시명 → 내부 코드)
            const sidoCode = REGION_CODE_MAP[sido] || sido;

            const userInput = {
                sido: sidoCode,  // 변환된 코드 (인천광역시 → 인천)
                sigungu: sigungu,
                industry: selectedIndustry,
                budget_excl_rent: selectedBudget,
                needs: needs
            };

            console.log('📊 사용자 입력 (변환됨):', userInput);
            console.log('🌍 원본 지역:', sido, '→', '변환된 지역:', sidoCode);
            console.log('🚀 API 호출 시작...');

            // API 호출 - user_input 객체 제거
            const response = await axios.post('/api/recommendations', userInput, {
                timeout: 1200000 // 10분 (600초) 타임아웃으로 연장
            });

            console.log('✅ API 호출 완료:', response.status);

            console.log('📄 API 응답:', response.data);

            if (response.data.success) {
                console.log('🎉 성공! 결과 표시 시작...');
                displayResults(response.data.data);
            } else {
                console.log('❌ API 응답 실패:', response.data.error);
                showError(response.data.error || '알 수 없는 오류가 발생했습니다.');
            }

        } catch (error) {
            console.error('API 호출 오류:', error);
            
            let errorMessage = '서버 연결에 실패했습니다.';
            if (error.response) {
                errorMessage = error.response.data?.error || `서버 오류 (${error.response.status})`;
            } else if (error.request) {
                errorMessage = '네트워크 연결을 확인해주세요.';
            } else if (error.code === 'ECONNABORTED') {
                errorMessage = '요청 시간이 초과되었습니다. 다시 시도해주세요.';
            }
            
            showError(errorMessage);
        }
    }
    // 점수(0~100) → 티어/색/문구/배경색
    function scoreStyle(score100) {
        const s = Number.isFinite(score100) ? score100 : 0;

        if (s >= 75) {
            return {
            tier: 'high',
            badgeText: '적극 추천',
            badgeClass: 'bg-green-100 text-green-700',
            barClass: 'bg-green-500',          // 진행바 색
            textClass: 'text-green-100',       // 헤더 안 숫자 글자색(필요시)
            };
        }
        if (s >= 50) {
            return {
            tier: 'mid',
            badgeText: '보통',
            badgeClass: 'bg-amber-100 text-amber-700',
            barClass: 'bg-amber-400',
            textClass: 'text-white',
            };
        }
        return {
            tier: 'low',
            badgeText: '재검토',
            badgeClass: 'bg-rose-100 text-rose-700',
            barClass: 'bg-rose-500',
            textClass: 'text-white',
        };
    }

    // 결과 표시 함수들 (기존과 동일)
    function displayResults(data) {
        console.log('🎨 Display Results 시작');
        console.log('📊 받은 데이터:', data);
        console.log("✅ 추천 브랜드 점수 배열:", data.recommended_brands.map(b => ({
        name: b.brand_name,
        raw: b.rerank_score,
        score100: b.score_100
        })));
        const { user_input, recommended_brands, analysis_summary } = data;
        console.log('🔎 [DBG] 첫번째 브랜드 summary_report:', recommended_brands?.[0]?.summary_report);
        console.log('🔎 [DBG] 키워드 존재여부:',
            Array.isArray(recommended_brands?.[0]?.summary_report?.["브랜드_소개_및_강점_키워드"]),
            Array.isArray(recommended_brands?.[0]?.summary_report?.["투자분석_키워드"]),
            Array.isArray(recommended_brands?.[0]?.summary_report?.["실행전략_키워드"])
        );
        hideAllSections();
        
        console.log('📋 브랜드 개수:', recommended_brands.length);
        
        resultSection.innerHTML = `
            <!-- 분석 완료 헤더 -->
            <div class="bg-green-50 border border-green-200 rounded-lg p-6">
                <div class="flex items-center">
                    <svg class="w-8 h-8 text-green-600 mr-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path>
                    </svg>
                    <div>
                        <h2 class="text-xl font-semibold text-green-900">분석 완료!</h2>
                        <p class="text-green-700">${recommended_brands.length}개 브랜드를 분석했습니다.</p>
                    </div>
                </div>
            </div>

            <!-- 분석 요약 -->
            ${analysis_summary ? `
            <div class="bg-white rounded-lg shadow-sm p-6">
                <h3 class="text-lg font-semibold mb-4 text-gray-900">📊 종합 분석 요약</h3>
                <div class="whitespace-pre-line text-gray-700 leading-relaxed">
                    ${escapeHtml(analysis_summary)}
                </div>
            </div>
            ` : `
            <div class="bg-blue-50 border border-blue-200 rounded-lg p-6">
                <h3 class="text-lg font-semibold mb-4 text-blue-900">📊 사용자 입력 정보</h3>
                <div class="grid grid-cols-2 gap-4 text-sm">
                    <div><span class="font-medium">지역:</span> ${user_input.sido} ${user_input.sigungu}</div>
                    <div><span class="font-medium">업종:</span> ${user_input.industry}</div>
                    <div><span class="font-medium">예산:</span> ${user_input.budget_excl_rent}만원</div>
                    <div><span class="font-medium">니즈:</span> ${escapeHtml(user_input.needs)}</div>
                </div>
            </div>
            `}

            <!-- 브랜드별 상세 분석 -->
            <div class="space-y-6">
                <h3 class="text-lg font-semibold text-gray-900">🏪 추천 브랜드 상세 분석</h3>
                ${recommended_brands.map((brand, index) => createBrandCard(brand, index + 1)).join('')}
            </div>

            <!-- 새로운 분석 버튼 -->
            <div class="text-center bg-orange-50 rounded-lg p-6">
                <button onclick="resetForm()" class="submit-btn">
                    새로운 조건으로 다시 분석하기
                </button>
            </div>
        `;
        
        resultSection.classList.remove('hidden');
        resultSection.scrollIntoView({ behavior: 'smooth' });
    }

    // 브랜드 카드 생성 (매칭 점수 제거)
    function createBrandCard(brand, rank) {
        const st = scoreStyle(brand.score_100);  // ⬅️ 추가
        const recommendationBadge = {
            'high': { color: 'bg-green-100 text-green-800', text: '적극 추천' },
            'medium': { color: 'bg-orange-100 text-orange-800', text: '검토 권장' },
            'low': { color: 'bg-red-100 text-red-800', text: '신중 검토' }
        }[brand.recommendation_level] || { color: 'bg-gray-100 text-gray-800', text: '분석 중' };

        return `
            <div class="bg-white rounded-lg shadow-sm border overflow-hidden">
                <!-- 헤더 -->
                <div class="bg-gradient-to-r from-orange-500 to-orange-600 text-white p-6">
                    <div class="flex justify-between items-start">
                    <div>
                        <h4 class="text-xl font-bold flex items-center">
                        <span class="bg-white text-orange-600 rounded-full w-8 h-8 flex items-center justify-center text-lg font-bold mr-3">
                            ${rank}
                        </span>
                        ${escapeHtml(brand.brand_name)}
                        </h4>
                        <p class="mt-2 text-orange-100">
                        브랜드 번호: ${escapeHtml(brand.brand_no)}
                        </p>

                        ${brand.score_100 != null ? `
                        <div class="mt-3">
                            <div class="text-sm ${st.textClass}">추천 점수:
                            <span class="font-semibold">${brand.score_100}</span> / 100
                            <span class="text-xs opacity-80 ml-1">(${brand.rerank_score?.toFixed?.(2)})</span>
                            </div>
                            <div class="w-full h-2 bg-white/40 rounded">
                            <div class="h-2 ${st.barClass} rounded" style="width:${brand.score_100}%"></div>
                            </div>
                        </div>
                        ` : ``}
                    </div>

                    <!-- 우상단 배지: 점수 기반 -->
                    <div class="text-right">
                        <div class="mt-1 px-3 py-1 ${st.badgeClass} rounded-full text-sm font-medium">
                        ${st.badgeText}
                        </div>
                    </div>
                    </div>
                </div>

                <!-- 본문 -->
                <div class="p-6 space-y-6">
                    ${createSummaryReportSection(brand.summary_report)}
                    ${brand.news_analysis ? createNewsAnalysisSection(brand.news_analysis) : ''}
                </div>
            </div>
        `;
    }

    // 1차 지표 분석 섹션 제거됨 - 사용자 요청에 의해

    // 요약 텍스트를 적당히 리스트/문단으로 렌더
    function _toListOrParagraphs(text) {
    if (!text) return '';
    const lines = String(text).split(/\r?\n/).map(s => s.trim());

    let html = '';
    let inList = false;

    const openList = () => {
        if (!inList) {
        html += '<div class="space-y-1 text-gray-700">'; // ✅ 불릿 없는 그룹
        inList = true;
        }
    };
    const closeList = () => {
        if (inList) {
        html += '</div>';
        inList = false;
        }
    };

    for (const raw of lines) {
        if (!raw) {
        closeList();
        continue;
        }

        if (raw.startsWith('## ')) {
        closeList();
        html += `<h4 class="text-md font-bold text-gray-800 mt-4">${escapeHtml(raw.slice(3))}</h4>`;
        continue;
        }

        if (raw.startsWith('✅')) {
        openList();
        html += `<div>${escapeHtml(raw)}</div>`;
        continue;
        }

        const mBullet = raw.match(/^([•\-·])\s+(.*)$/);
        if (mBullet) {
        openList();
        html += `<div>${escapeHtml(mBullet[2])}</div>`;
        continue;
        }

        const mNum = raw.match(/^(\d+)\.\s+(.*)$/);
        if (mNum) {
        openList();
        html += `<div>${escapeHtml(mNum[2])}</div>`;
        continue;
        }

        closeList();
        html += `<p class="text-gray-700">${escapeHtml(raw)}</p>`;
    }

    closeList();
    return html;
    }


    // 섹션 블록 공통 UI (배지형 제목 + 내용)
    function _sectionBlock({ icon, title, body }) {
    if (!body) return '';
    return `
        <div class="rounded-lg border border-gray-200 p-4 bg-white">
        <div class="inline-flex items-center gap-2 text-gray-800 font-semibold mb-3">
            <span class="inline-flex items-center justify-center text-xs px-2 py-1 rounded-full bg-gray-100 border border-gray-200">${icon}</span>
            <span class="tracking-[-0.01em]">${title}</span>
        </div>
        ${_toListOrParagraphs(body)}
        </div>
    `;
    }

    

    function _kpiStrip(S) {
    const arr1 = Array.isArray(S["브랜드_소개_및_강점_키워드"]) ? S["브랜드_소개_및_강점_키워드"].slice(0, 5) : [];
    const arr2 = Array.isArray(S["투자분석_키워드"]) ? S["투자분석_키워드"].slice(0, 5) : [];
    const arr3 = Array.isArray(S["실행전략_키워드"]) ? S["실행전략_키워드"].slice(0, 5) : [];

    const chip = (kw, cls) => `
        <span class="inline-block ${cls} text-sm px-2.5 py-1 rounded-full border">
        ${escapeHtml(kw)}
        </span>
    `;

    const block = (label, arr, color) => `
        <div class="bg-gray-50 border border-gray-200 rounded-lg p-3">
        <div class="text-xs text-gray-500 text-center mb-1">${label}</div>
        <div class="flex flex-wrap gap-1 justify-center">
            ${arr.length ? arr.map(kw => chip(kw, `bg-${color}-50 text-${color}-700 border-${color}-200`)).join('') : '—'}
        </div>
        </div>
    `;

    // 키워드가 하나도 없으면 기존(투자/매출/리스크 첫줄)로 폴백
    const noKeywords = arr1.length + arr2.length + arr3.length === 0;
    if (noKeywords) {
        const first = (t) => (typeof t === "string" ? t.split(/\r?\n/)[0].slice(0, 60) : "—");
        return `
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4">
            <div class="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center">
            <div class="text-xs text-gray-500">투자 핵심</div>
            <div class="text-sm font-semibold text-gray-900 mt-1">${escapeHtml(first(S.investment_analysis))}</div>
            </div>
            <div class="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center">
            <div class="text-xs text-gray-500">매출/수익 핵심</div>
            <div class="text-sm font-semibold text-green-700 mt-1">${escapeHtml(first(S.profitability_analysis))}</div>
            </div>
            <div class="bg-gray-50 border border-gray-200 rounded-lg p-3 text-center">
            <div class="text-xs text-gray-500">리스크 핵심</div>
            <div class="text-sm font-semibold text-blue-700 mt-1">${escapeHtml(first(S.risk_analysis))}</div>
            </div>
        </div>
        `;
    }

    return `
        <div class="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-4">
        ${block("브랜드 강점", arr1, "orange")}
        ${block("투자 분석", arr2, "green")}
        ${block("실행 전략", arr3, "blue")}
        </div>
    `;
    }


    // function _keywordBlock(title, keywords) {
    //     if (!Array.isArray(keywords) || keywords.length === 0) return '';
    //     return `
    //         <div class="rounded-lg border border-gray-200 p-4 bg-white">
    //         <div class="inline-flex items-center gap-2 text-gray-800 font-semibold mb-3">
    //             <span class="inline-flex items-center justify-center text-xs px-2 py-1 rounded-full bg-gray-100 border border-gray-200">#</span>
    //             <span class="tracking-[-0.01em]">${title}</span>
    //         </div>
    //         <div class="flex flex-wrap gap-2">
    //             ${keywords.map(kw => `
    //             <span class="inline-block bg-orange-50 text-orange-700 text-sm px-3 py-1 rounded-full border border-orange-200">
    //                 ${escapeHtml(kw)}
    //             </span>`).join('')}
    //         </div>
    //         </div>
    //     `;
    //     }


    // ✨ 깔끔한 요약 섹션
    function createSummaryReportSection(summaryReport) {
    const S = summaryReport || {};
    return `
        <div>
        <div class="flex items-center justify-between mb-3">
            <h5 class="text-lg font-semibold flex items-center text-gray-900 tracking-[-0.01em]">
            <svg class="w-5 h-5 mr-2 text-green-600" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M6.267 3.455a3.066 3.066 0 001.745-.723 3.066 3.066 0 013.976 0 3.066 3.066 0 001.745.723 3.066 3.066 0 012.812 2.812c.051.643.304 1.254.723 1.745a3.066 3.066 0 010 3.976 3.066 3.066 0 00-.723 1.745 3.066 3.066 0 01-2.812 2.812 3.066 3.066 0 00-1.745.723 3.066 3.066 0 01-3.976 0 3.066 3.066 0 00-1.745-.723 3.066 3.066 0 01-2.812-2.812 3.066 3.066 0 00-.723-1.745 3.066 3.066 0 010-3.976 3.066 3.066 0 00.723-1.745 3.066 3.066 0 012.812-2.812zm7.44 5.252a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clip-rule="evenodd"/></svg>
            AI 요약 보고서
            </h5>
        </div>

        ${_kpiStrip(S)}

        <div class="space-y-4">
            ${_sectionBlock({ icon:'🏢', title:'브랜드 요약',          body: S.brand_summary })}
            ${_sectionBlock({ icon:'💰', title:'창업비용 (요약+세부)', body: S.investment_analysis })}
            ${_sectionBlock({ icon:'📈', title:'매출/수익성 분석',     body: S.profitability_analysis })}
            ${_sectionBlock({ icon:'🏪', title:'브랜드 강점/시장',     body: S.market_analysis })}
            ${_sectionBlock({ icon:'⚠️', title:'리스크',               body: S.risk_analysis })}
            ${_sectionBlock({ icon:'👍', title:'권고사항',             body: S.recommendation })}
        </div>
        </div>
    `;
    }

    // 최신 뉴스 요약 섹션
    function createNewsAnalysisSection(newsAnalysis) {
        return `
            <div>
                <h5 class="text-lg font-semibold mb-3 flex items-center text-gray-900 tracking-[-0.01em]">
                    <svg class="w-5 h-5 mr-2 text-red-600" fill="currentColor" viewBox="0 0 20 20">
                        <path fill-rule="evenodd" d="M2 5a2 2 0 012-2h8a2 2 0 012 2v10a2 2 0 002 2H4a2 2 0 01-2-2V5zm3 1h6v4H5V6zm6 6H5v2h6v-2z" clip-rule="evenodd"></path>
                        <path d="M15 7h1a2 2 0 012 2v5.5a1.5 1.5 0 01-3 0V7z"></path>
                    </svg>
                    해당 브랜드 최신 뉴스 요약 - ${newsAnalysis.news_items.length}개 뉴스
                </h5>
                
                <!-- 전체 뉴스 요약 -->
                <div class="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
                    <h6 class="font-semibold text-red-800 mb-2">📰 종합 뉴스 분석</h6>
                    <div class="text-gray-700 leading-relaxed whitespace-pre-line">
                        ${escapeHtml(newsAnalysis.overall_summary)}
                    </div>
                </div>



                <!-- 개별 뉴스 및 요약 -->
                <div class="space-y-3">
                    <h6 class="font-semibold text-gray-800">📝 개별 뉴스 분석 (${newsAnalysis.news_items.length}개)</h6>
                    ${newsAnalysis.news_items.map((news, index) => `
                        <div class="bg-gray-50 border border-gray-200 rounded-lg p-3">
                            <div class="flex justify-between items-start mb-2">
                                <h6 class="font-medium text-gray-800 flex-1 mr-4">
                                    <a href="${news.link}" target="_blank" class="hover:text-orange-600 hover:underline">
                                        ${escapeHtml(news.title)}
                                    </a>
                                </h6>
                                <span class="text-sm text-gray-500 whitespace-nowrap">
                                    ${formatDate(news.pubDate)}
                                </span>
                            </div>
                            <div class="text-gray-600 leading-relaxed">
                                ${escapeHtml(newsAnalysis.individual_summaries[index] || news.description)}
                            </div>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    }

    // UI 유틸리티 함수들
    function hideAllSections() {
        loadingSection.classList.add('hidden');
        resultSection.classList.add('hidden');
        errorSection.classList.add('hidden');
    }

    function showLoadingSection() {
        loadingSection.classList.remove('hidden');
        submitBtn.disabled = true;
        submitText.textContent = '분석 중...';
    }

    function showError(message) {
        hideAllSections();
        document.getElementById('errorMessage').textContent = message;
        errorSection.classList.remove('hidden');
        resetSubmitButton();
        errorSection.scrollIntoView({ behavior: 'smooth' });
    }

    function resetSubmitButton() {
        submitBtn.disabled = false;
        submitText.textContent = '추천 실행';
    }

    // 전역 함수로 노출
    window.resetForm = function() {
        form.reset();
        hideAllSections();
        resetSubmitButton();
        
        // 선택값 초기화
        selectedIndustry = '';
        selectedBudget = '';
        
        // 모든 버튼 선택 해제
        document.querySelectorAll('.industry-btn, .budget-btn').forEach(btn => {
            btn.classList.remove('selected');
        });
        
        // 기타 버튼 초기화
        const otherBtn = document.querySelector('[data-value="기타"]');
        if (otherBtn) {
            otherBtn.textContent = '기타';
            otherBtn.dataset.value = '기타';
        }
        
        // 지역 선택 초기화
        sigunguSelect.innerHTML = '<option value="">시/도를 먼저 선택하세요</option>';
        sigunguSelect.disabled = true;
        
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    // 유틸리티 함수들
    function escapeHtml(text) {
        if (!text) return '';
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, function(m) { return map[m]; });
    }

    function formatDate(dateString) {
        if (!dateString) return '';
        try {
            const date = new Date(dateString);
            return date.toLocaleDateString('ko-KR', {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            });
        } catch {
            return dateString;
        }
    }

    // 초기화
    populateSidoOptions();
    createOtherIndustryModal();
});

// 페이지 로드 시 스크롤 위치 초기화
window.addEventListener('load', function() {
    window.scrollTo(0, 0);
});