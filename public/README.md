# FRAI 프론트엔드 코드

## 개요
FRAI (Franchise Recommendation AI) 프랜차이즈 브랜드 추천 서비스의 프론트엔드 코드입니다.

## 파일 구성

### 📄 index.html
- **메인 HTML 파일**
- 40-50대 사용자를 위한 접근성 개선 UI
- 프랜차이즈 브랜드 추천 폼과 결과 화면
- Tailwind CSS + FontAwesome 사용

**주요 기능:**
- 지역 선택 (시도/시군구)
- 업종 선택 (커피, 치킨, 한식 등)
- 예산 선택 (임대료 제외)
- 창업 니즈 입력
- 실시간 분석 결과 표시

### 📄 app.js (24KB)
- **메인 JavaScript 파일**
- 완전한 프론트엔드 로직 구현
- 실제 Databricks 연동 및 OpenAI o1-mini 모델 기반 분석

**주요 기능:**
1. **지역 데이터 관리**
   - 한국 전국 시도-시군구 매핑
   - 지역명 정규화 (인천광역시 → 인천)

2. **업종 관리**
   - 기본 업종 + 기타 업종 모달
   - 30개 추가 업종 선택 가능

3. **API 연동**
   - `/api/recommendations` 엔드포인트 호출
   - 240초 타임아웃 설정
   - 실시간 로딩 상태 표시

4. **결과 표시**
   - 3단계 분석 결과 (지표분석 + AI요약보고서 + 뉴스분석)
   - 브랜드별 상세 카드
   - 추천 레벨 배지 (적극추천/검토권장/신중검토)

### 📄 styles.css (5KB)
- **커스텀 CSS 스타일**
- 40-50대 접근성 최적화
- 반응형 디자인 + 다크모드 지원

**주요 스타일:**
- 큰 폰트 사이즈 (16px 기본)
- 향상된 버튼 및 폼 요소
- 카드 애니메이션 및 호버 효과
- 접근성 개선 (고대비, 포커스 표시)

## 기술 스택
- **HTML5** + **Vanilla JavaScript**
- **Tailwind CSS** (CDN)
- **FontAwesome** (아이콘)
- **Axios** (HTTP 클라이언트)

## 사용법

### 1. 단독 실행 (정적 서버)
```bash
# 웹 서버에서 index.html 서빙
# app.js의 API 경로를 실제 백엔드 URL로 수정 필요
```

### 2. Hono 백엔드와 연동
```bash
# public/static/ 디렉토리에 파일 배치
cp index.html /path/to/hono/public/
cp app.js /path/to/hono/public/static/
cp styles.css /path/to/hono/public/static/
```

### 3. API 연동 설정
app.js에서 백엔드 URL 수정:
```javascript
// 로컬 개발
const response = await axios.post('/api/recommendations', userInput);

// 프로덕션
const response = await axios.post('https://your-api.com/api/recommendations', userInput);
```

## 데이터 흐름
1. **사용자 입력** → 폼 데이터 수집
2. **지역 정규화** → 시도명 변환 (광역시 → 축약형)
3. **API 호출** → POST `/api/recommendations`
4. **결과 파싱** → 3단계 분석 데이터 표시
5. **UI 업데이트** → 브랜드 카드 및 요약 정보

## 특징
- ✅ **실제 데이터 기반**: Databricks 11,876개 프랜차이즈
- ✅ **AI 분석**: OpenAI o1-mini 모델 활용
- ✅ **접근성 최적화**: 40-50대 사용자 친화적
- ✅ **반응형 디자인**: 모바일/태블릿 지원
- ✅ **뉴스 통합**: 네이버 뉴스 API 연동
- ✅ **세부 분석**: 브랜드별 6단계 요약보고서

## 백업 정보
- **원본 프로젝트**: FRAI 프랜차이즈 웹앱
- **백업 날짜**: 2025-08-29
- **버전**: 완성된 기능 구현 상태
- **상태**: 테스트 완료, 프로덕션 준비

---

이 프론트엔드 코드는 완전한 FRAI 서비스 구현을 위한 클라이언트 부분입니다.  
백엔드 API와 연동하여 실제 프랜차이즈 추천 서비스를 제공할 수 있습니다.