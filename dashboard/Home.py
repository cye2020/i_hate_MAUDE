"""
Streamlit 멀티페이지 대시보드 - 메인 홈페이지
"""

import streamlit as st
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from millify import millify


# ==================== 페이지 설정 ====================
st.set_page_config(
    page_title="MAUDE 데이터 분석 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# 초기화 시 한 번만 TODAY 설정
if 'TODAY' not in st.session_state:
    st.session_state.TODAY = datetime.now()

TODAY = st.session_state.TODAY

# ==================== 사이드바 ====================
with st.sidebar:
    st.image("dashboard/assets/logo.png", width='stretch')
    
    # 프로젝트 정보
    st.markdown("### 📌 프로젝트 정보")
    st.info("""
    **버전**: v1.0.0  
    **업데이트**: 2025-12-24  
    **환경**: Development
    """)
    
    st.markdown('---')

    with st.container(horizontal=True):
        year_range = 3
        year = st.selectbox(
            "년도",
            range(TODAY.year - year_range + 1, TODAY.year+1),
            index=year_range - 1,
            format_func=lambda x: f"{x}년",
            width="stretch"
        )
        st.space(1)  # 간격 추가
        month = st.selectbox(
            "월",
            range(1, 13),
            format_func=lambda x: f"{x:02d}월",
            width="stretch"
        )

    selected_date = datetime(year, month, 1)
    st.write(f"선택된 년월: {selected_date.strftime('%Y년 %m월')}")
    
    window = st.selectbox(
        label='관측 기간',
        options = [1, 3],
        index = 0,
        format_func=lambda op: f'{op}개월'
    )
    
    st.markdown("---")
    
    # 빠른 링크
    st.markdown("### 🔗 빠른 링크")
    st.markdown("""
    - [데이터 개요](#data-overview)
    - [분석 대시보드](#analytics)
    - [모델 성능](#model-performance)
    """)

# ==================== 메인 콘텐츠 ====================

# 헤더
# st.title("🏠 홈 대시보드")
# st.markdown("데이터 파이프라인과 ML 모델 모니터링을 위한 통합 대시보드입니다.")

# 메인 영역 상단의 탭
overview_tab, eda_tab, cluster_tab = st.tabs(["Overview", "Detailed Analysis", "Clustering Reports"])

# 탭 내용
with overview_tab:
    st.session_state.current_tab = "Overview"
    st.header('Overview Dashboard')

    # KPI 메트릭 (3열 레이아웃)
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="📁 총 이상 사례 보고 건수",
            value="1,234,567",
            delta="12.5%",
            delta_arrow='down',
            delta_color='inverse'
        )

    with col2:
        st.metric(
            label="⚙️ 파이프라인 상태",
            value="정상",
            delta="100% Uptime"
        )

    with col3:
        st.metric(
            label="🤖 모델 정확도",
            value="94.2%",
            delta="↑ 2.3%"
        )

    st.markdown("---")

with eda_tab:
    st.session_state.current_tab = "EDA"
    st.header("Detailed Analysis")
    
    # ==================== 주요 기능 안내 ====================
    st.subheader("📚 주요 기능")

    col1, col2 = st.columns(2)

    with col1:
        with st.container(border=True):
            st.markdown("### 📊 데이터 개요")
            st.markdown("""
            - Bronze/Silver/Gold 데이터 레이어 현황
            - 데이터 품질 지표
            - 최근 업데이트 이력
            """)
            if st.button("데이터 개요 보기", key="btn_data", width='stretch'):
                st.switch_page("pages/1_📊_Data_Overview.py")

    with col2:
        with st.container(border=True):
            st.markdown("### 📈 분석 대시보드")
            st.markdown("""
            - 인터랙티브 차트 및 시각화
            - 트렌드 분석
            - 커스텀 필터링
            """)
            if st.button("분석 대시보드 보기", key="btn_analytics", width='stretch'):
                st.switch_page("pages/2_📈_Analytics.py")

    col3, col4 = st.columns(2)

    with col3:
        with st.container(border=True):
            st.markdown("### 🤖 모델 성능")
            st.markdown("""
            - 모델 정확도 및 성능 지표
            - 학습 이력
            - A/B 테스트 결과
            """)
            if st.button("모델 성능 보기", key="btn_model", width='stretch'):
                st.switch_page("pages/3_🤖_Model_Performance.py")

    with col4:
        with st.container(border=True):
            st.markdown("### ⚙️ 설정")
            st.markdown("""
            - 데이터 소스 설정
            - 알림 설정
            - 사용자 권한 관리
            """)
            if st.button("설정 보기", key="btn_settings", width='stretch'):
                st.switch_page("pages/4_⚙️_Settings.py")

    st.markdown("---")
    

with cluster_tab:
    st.session_state.current_tab = "Cluster"
    st.header("Cluster Reports")

    # ==================== 최근 활동 ====================
    st.subheader("📝 최근 활동")

    with st.expander("최근 24시간 활동 내역", expanded=True):
        # 샘플 활동 데이터
        activities = [
            {"time": "2시간 전", "event": "데이터 전처리 완료", "status": "✅"},
            {"time": "5시간 전", "event": "모델 학습 시작", "status": "🔄"},
            {"time": "8시간 전", "event": "새 데이터 수집 (1,500건)", "status": "✅"},
            {"time": "12시간 전", "event": "배치 작업 완료", "status": "✅"},
        ]
        
        for activity in activities:
            col1, col2, col3 = st.columns([1, 5, 1])
            with col1:
                st.markdown(f"**{activity['time']}**")
            with col2:
                st.markdown(activity['event'])
            with col3:
                st.markdown(activity['status'])

    st.markdown("---")

# ==================== 시스템 상태 ====================
st.subheader("🖥️ 시스템 상태")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**데이터 파이프라인**")
    st.progress(0.95)
    st.caption("95% - 정상 작동 중")

with col2:
    st.markdown("**모델 서빙**")
    st.progress(1.0)
    st.caption("100% - 정상")

with col3:
    st.markdown("**데이터베이스**")
    st.progress(0.87)
    st.caption("87% - 여유 공간")

# ==================== 푸터 ====================
st.markdown("---")
st.caption(f"최종 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 버전: 1.0.0")