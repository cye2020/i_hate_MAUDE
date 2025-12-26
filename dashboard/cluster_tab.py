# cluster_tab.py
import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
from utils.analysis_cluster import cluster_check
from utils.constants import ColumnNames, Defaults, ChartStyles
from utils.data_utils import get_year_month_expr

def show(
    filters=None,
    lf: pl.LazyFrame = None
):
    st.title("🔍 Clustering Reports")

    # 필터 값 사용 (sidebar에서 전달)
    date_range = filters.get("date_range", None)

    # date_range를 문자열 리스트로 변환
    selected_dates = []
    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range

        # 시작일부터 종료일까지 월별 리스트 생성
        current = start_date
        while current <= end_date:
            selected_dates.append(current.strftime("%Y-%m"))
            current = current + relativedelta(months=1)

    # year_month 표현식 생성 (재사용)
    year_month_expr = get_year_month_expr(lf, ColumnNames.DATE_RECEIVED)

    # 사이드바에서 선택된 값 가져오기
    selected_cluster = filters.get("selected_cluster")
    top_n = filters.get("top_n", Defaults.TOP_N)

    # 선택된 클러스터가 없으면 경고 표시
    if selected_cluster is None:
        st.warning("클러스터를 선택해주세요.")
        return

    # 클러스터 분석 실행
    cluster_data = cluster_check(
        _lf=lf,
        cluster_name=selected_cluster,
        cluster_col=ColumnNames.CLUSTER,
        component_col=ColumnNames.PROBLEM_COMPONENTS,
        event_col=ColumnNames.PATIENT_HARM,
        date_col=ColumnNames.DATE_RECEIVED,
        selected_dates=selected_dates,
        selected_manufacturers=None,
        selected_products=None,
        top_n=top_n,
        _year_month_expr=year_month_expr
    )

    # 1. 전체 요약 메트릭
    st.subheader(f"📊 클러스터: {selected_cluster}")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("전체 케이스", f"{cluster_data['total_count']:,}")
    with col2:
        st.metric("사망", f"{cluster_data['harm_summary']['total_deaths']:,}",
                  delta=None, delta_color="inverse")
    with col3:
        st.metric("중증 부상", f"{cluster_data['harm_summary']['total_serious_injuries']:,}",
                  delta=None, delta_color="inverse")
    with col4:
        st.metric("경증 부상", f"{cluster_data['harm_summary']['total_minor_injuries']:,}",
                  delta=None, delta_color="inverse")

    st.markdown("---")

    # 2. 환자 피해 분포 (파이 차트)
    st.subheader("🎯 환자 피해 분포")

    harm_summary = cluster_data['harm_summary']

    # 값이 0보다 큰 항목만 필터링
    harm_data = [
        ('Death', harm_summary['total_deaths'], ChartStyles.DANGER_COLOR),
        ('Serious Injury', harm_summary['total_serious_injuries'], ChartStyles.WARNING_COLOR),
        ('Minor Injury', harm_summary['total_minor_injuries'], '#ffd700'),
        ('No Harm', harm_summary['total_no_injuries'], ChartStyles.SUCCESS_COLOR),
        ('Unknown', harm_summary.get('total_unknown', 0), '#9CA3AF')  # 회색
    ]

    # 값이 0보다 큰 항목만 선택
    filtered_harm_data = [(label, value, color) for label, value, color in harm_data if value > 0]

    if filtered_harm_data:
        harm_labels = [item[0] for item in filtered_harm_data]
        harm_values = [item[1] for item in filtered_harm_data]
        harm_colors = [item[2] for item in filtered_harm_data]
    else:
        # 모든 값이 0인 경우 기본값 사용
        harm_labels = ['Death', 'Serious Injury', 'Minor Injury', 'No Harm', 'Unknown']
        harm_values = [
            harm_summary['total_deaths'],
            harm_summary['total_serious_injuries'],
            harm_summary['total_minor_injuries'],
            harm_summary['total_no_injuries'],
            harm_summary.get('total_unknown', 0)
        ]
        harm_colors = [
            ChartStyles.DANGER_COLOR,
            ChartStyles.WARNING_COLOR,
            '#ffd700',
            ChartStyles.SUCCESS_COLOR,
            '#9CA3AF'
        ]

    fig_pie = go.Figure(data=[go.Pie(
        labels=harm_labels,
        values=harm_values,
        hole=0.3,
        marker=dict(colors=harm_colors)
    )])

    fig_pie.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=40, b=20),
        title="환자 피해 분포"
    )

    st.plotly_chart(fig_pie, width='stretch')

    st.markdown("---")

    # 3. 상위 부품 분석 (막대 차트)
    st.subheader(f"🔧 상위 {top_n}개 문제 부품")

    top_components = cluster_data['top_components']

    if len(top_components) > 0:
        fig_bar = px.bar(
            top_components,
            x='count',
            y=ColumnNames.PROBLEM_COMPONENTS,
            orientation='h',
            text='ratio',
            title=f"상위 {top_n}개 문제 부품 (비율 %)",
            labels={
                'count': '발생 건수',
                ColumnNames.PROBLEM_COMPONENTS: '부품명',
                'ratio': '비율 (%)'
            }
        )

        fig_bar.update_traces(
            texttemplate='%{text}%',
            textposition='outside',
            marker_color=ChartStyles.PRIMARY_COLOR
        )

        fig_bar.update_layout(
            height=max(400, len(top_components) * 30),
            margin=dict(l=20, r=20, t=60, b=20),
            yaxis={'categoryorder': 'total ascending'}
        )

        st.plotly_chart(fig_bar, width='stretch')

        # 데이터 테이블 표시
        with st.expander("📋 상세 데이터 보기"):
            st.dataframe(
                top_components,
                width='stretch',
                hide_index=True
            )
    else:
        st.info("해당 클러스터에는 부품 정보가 없습니다.")

    st.markdown("---")

    # 4. 시계열 분석 (라인 차트)
    st.subheader("📈 시계열 분석")

    time_series = cluster_data['time_series']

    if len(time_series) > 0:
        fig_line = px.line(
            time_series,
            x='year_month',
            y='count',
            title=f"클러스터 '{selected_cluster}' 월별 발생 추이",
            labels={
                'year_month': '년-월',
                'count': '발생 건수'
            },
            markers=True
        )

        fig_line.update_traces(
            line_color=ChartStyles.PRIMARY_COLOR,
            line_width=3,
            marker=dict(size=8)
        )

        fig_line.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=60, b=80),
            hovermode='x unified',
            xaxis_tickangle=-45
        )

        st.plotly_chart(fig_line, width='stretch')

        # 통계 요약
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("평균 월별 발생", f"{time_series['count'].mean():.1f}")
        with col2:
            st.metric("최대 월별 발생", f"{time_series['count'].max()}")
        with col3:
            st.metric("최소 월별 발생", f"{time_series['count'].min()}")
    else:
        st.info("시계열 데이터가 없습니다.")
