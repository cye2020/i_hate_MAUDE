# spike_tab.py
import polars as pl
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Optional, List

from dashboard.utils.analysis import perform_spike_detection, get_spike_time_series
from dashboard.utils.constants import ColumnNames


def show(filters=None, lf: pl.LazyFrame = None):
    """
    Spike Detection 탭

    Args:
        filters: SidebarManager에서 생성된 필터 딕셔너리
            - date_range: (start_date, end_date) 튜플
            - as_of_month: 기준 월 (예: "2025-11")
            - window: 윈도우 크기 (1 또는 3)
            - min_c_recent: 최소 최근 케이스 수
            - z_threshold: Z-score 임계값
            - eps: Epsilon 값
            - alpha: 유의수준
            - correction: 다중검정 보정 방법
            - min_methods: 앙상블 최소 방법 수
        lf: MAUDE 데이터 LazyFrame
    """
    st.title("📈 Spike Detection")

    if lf is None:
        st.warning("데이터가 로드되지 않았습니다.")
        return

    # 필터가 없으면 기본값 사용
    if filters is None:
        filters = {}

    # 필터 값 추출
    as_of_month = filters.get('as_of_month', '2025-11')
    window = filters.get('window', 1)
    min_c_recent = filters.get('min_c_recent', 20)
    z_threshold = filters.get('z_threshold', 2.0)
    eps = filters.get('eps', 0.1)
    alpha = filters.get('alpha', 0.05)
    correction = filters.get('correction', 'fdr_bh')
    min_methods = filters.get('min_methods', 2)

    # 스파이크 탐지 수행 (기본값으로 미리 계산)
    with st.spinner("스파이크 탐지 분석 중..."):
        result_df = outlier_detect_check(
            lf=lf,
            window=window,
            min_c_recent=min_c_recent,
            z_threshold=z_threshold,
            eps=eps,
            alpha=alpha,
            correction=correction,
            min_methods=min_methods,
            month=as_of_month,
        )

    if result_df is None or len(result_df) == 0:
        st.info("분석할 데이터가 없습니다.")
        return

    # 결과 표시
    st.success(f"총 {len(result_df)}개의 키워드를 분석했습니다.")

    # 패턴별 요약
    pattern_counts = result_df.group_by("pattern").agg(pl.len().alias("count")).sort("count", descending=True)
    st.subheader("📊 패턴별 분포")

    col1, col2, col3, col4 = st.columns(4)
    pattern_map = {
        "severe": ("🔴 Severe", col1),
        "alert": ("🟠 Alert", col2),
        "attention": ("🟡 Attention", col3),
        "general": ("🟢 General", col4)
    }

    for pattern, (label, col) in pattern_map.items():
        count = pattern_counts.filter(pl.col("pattern") == pattern)
        count_val = count["count"][0] if len(count) > 0 else 0
        col.metric(label, count_val)

    # 스파이크 키워드만 필터링 (앙상블 기준)
    spike_df = result_df.filter(pl.col("is_spike_ensemble") == True)

    # 시계열 데이터 준비 (12개월)
    end_date = datetime.strptime(as_of_month, "%Y-%m")
    start_date = end_date - relativedelta(months=11)
    start_month = start_date.strftime("%Y-%m")

    # 1. 이상 탐지 그래프 (비율 시계열)
    st.subheader("📈 키워드 비율 추이 (Anomaly Detection)")

    # TopN 필터 및 정렬 기준 선택
    col_filter1, col_filter2, col_filter3 = st.columns([1, 1, 2])
    with col_filter1:
        top_n_chart = st.number_input(
            "표시할 키워드 수",
            min_value=1,
            max_value=20,
            value=13,
            step=1,
            key="top_n_chart"
        )

    with col_filter2:
        sort_by = st.selectbox(
            "정렬 기준",
            options=["ratio", "n_methods", "score_pois", "C_recent"],
            format_func=lambda x: {
                "ratio": "비율 (Ratio)",
                "n_methods": "스파이크 방법 수",
                "score_pois": "Poisson 점수",
                "C_recent": "최근 보고수"
            }[x],
            index=0,
            key="sort_by_chart"
        )

    # TopN에 맞춰 키워드 선택 (정렬 기준 적용)
    top_keywords_filtered = get_top_n_keywords(
        result_df=result_df,
        spike_df=spike_df,
        top_n=top_n_chart,
        sort_by=sort_by
    )

    # 필터링된 키워드로 시계열 데이터 가져오기
    if len(top_keywords_filtered) > 0:
        ts_df_filtered = get_spike_time_series(
            _lf=lf,
            keywords=top_keywords_filtered,
            start_month=start_month,
            end_month=as_of_month
        )

        if len(ts_df_filtered) > 0:
            fig = create_spike_chart(ts_df_filtered, z_threshold, as_of_month, window)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("시계열 데이터가 없습니다.")
    else:
        st.info("시계열 데이터가 없습니다.")

    # 2. 전체 결과 테이블 (패턴별 필터링 가능)
    st.subheader("📋 전체 분석 결과")

    # 패턴 필터
    col_pattern, col_topn = st.columns([2, 2])
    with col_pattern:
        pattern_filter = st.multiselect(
            "패턴 필터",
            options=["severe", "alert", "attention", "general"],
            default=["severe", "alert", "attention"],
            format_func=lambda x: {
                "severe": "🔴 Severe",
                "alert": "🟠 Alert",
                "attention": "🟡 Attention",
                "general": "🟢 General"
            }[x]
        )

    with col_topn:
        top_n_table = st.number_input(
            "표시할 행 수",
            min_value=10,
            max_value=100,
            value=50,
            step=10,
            key="top_n_table"
        )

    # 필터링된 결과 테이블
    filtered_result = result_df.filter(pl.col("pattern").is_in(pattern_filter))
    display_all_df = prepare_spike_table(filtered_result.head(top_n_table))
    st.dataframe(display_all_df, width='stretch', height=600)

    # 3. 스파이크 키워드 요약
    if len(spike_df) > 0:
        st.subheader(f"⚠️ 스파이크 탐지 키워드 요약 ({len(spike_df)}개)")
        spike_summary_df = prepare_spike_table(spike_df.head(20))
        st.dataframe(spike_summary_df, width='stretch', height=400)
    else:
        st.info("탐지된 스파이크가 없습니다.")

    # 4. 전체 데이터 다운로드
    st.subheader("📥 전체 결과 다운로드")
    csv = result_df.write_csv()
    st.download_button(
        label="CSV 다운로드",
        data=csv,
        file_name=f"spike_detection_{as_of_month}_w{window}.csv",
        mime="text/csv"
    )


def get_top_n_keywords(
    result_df: pl.DataFrame,
    spike_df: pl.DataFrame,
    top_n: int,
    sort_by: str = "ratio"
) -> List[str]:
    """TopN 키워드를 정렬 기준에 따라 선택

    Args:
        result_df: 전체 결과 DataFrame
        spike_df: 스파이크만 필터링된 DataFrame
        top_n: 선택할 키워드 수
        sort_by: 정렬 기준 ("ratio", "n_methods", "score_pois", "C_recent")

    Returns:
        선택된 키워드 리스트
    """
    # 항상 전체 결과에서 선택 (스파이크가 적을 수 있음)
    if len(result_df) == 0:
        return []

    # 정렬 기준에 따라 정렬
    sorted_df = result_df.sort(sort_by, descending=True)

    # 상위 N개 키워드 추출
    top_keywords = sorted_df.head(top_n)["keyword"].to_list()

    return top_keywords


def outlier_detect_check(
    lf: pl.LazyFrame,
    window: int = 1,
    min_c_recent: int = 20,
    z_threshold: float = 2.0,
    eps: float = 0.1,
    alpha: float = 0.05,
    correction: str = 'fdr_bh',
    min_methods: int = 2,
    month: str = "2025-11",
) -> Optional[pl.DataFrame]:
    """
    스파이크 탐지 분석 수행

    Args:
        lf: MAUDE 데이터 LazyFrame
        window: 윈도우 크기 (1 또는 3)
        min_c_recent: 최소 최근 케이스 수
        z_threshold: Z-score 임계값
        eps: Epsilon 값 (z_log 계산용)
        alpha: 유의수준 (Poisson 검정용)
        correction: 다중검정 보정 방법 ('bonferroni', 'sidak', 'fdr_bh', None)
        min_methods: 앙상블 스파이크 판정 최소 방법 수
        month: 기준 월 (예: "2025-11")

    Returns:
        스파이크 탐지 결과 DataFrame
        컬럼: keyword, C_recent, C_base, ratio, z_log, score_pois,
              is_spike, is_spike_z, is_spike_p, n_methods, is_spike_ensemble, pattern
    """
    result_df = perform_spike_detection(
        _lf=lf,
        as_of_month=month,
        window=window,
        min_c_recent=min_c_recent,
        z_threshold=z_threshold,
        eps=eps,
        alpha=alpha,
        correction=correction,
        min_methods=min_methods,
    )

    return result_df


def create_spike_chart(
    ts_df: pl.DataFrame,
    z_threshold: float,
    as_of_month: str,
    window: int
) -> go.Figure:
    """
    스파이크 시계열 차트 생성

    Args:
        ts_df: 시계열 데이터 (columns: month, keyword, count, ratio)
        z_threshold: Z-score 임계값 (표시용)
        as_of_month: 기준 월
        window: 윈도우 크기

    Returns:
        Plotly Figure 객체
    """
    fig = go.Figure()

    # 키워드별로 라인 추가
    keywords = ts_df["keyword"].unique().to_list()

    # 색상 팔레트
    colors = [
        '#e6194b', '#3cb44b', '#ffe119', '#4363d8', '#f58231',
        '#911eb4', '#46f0f0', '#f032e6', '#bcf60c', '#fabebe',
        '#008080', '#e6beff', '#9a6324'
    ]

    for i, keyword in enumerate(keywords):
        keyword_data = ts_df.filter(pl.col("keyword") == keyword).sort("month")

        fig.add_trace(go.Scatter(
            x=keyword_data["month"].to_list(),
            y=keyword_data["ratio"].to_list(),
            mode='lines+markers',
            name=keyword,
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=6),
            hovertemplate='<b>%{fullData.name}</b><br>' +
                         'Month: %{x}<br>' +
                         'Ratio: %{y:.4f}%<br>' +
                         '<extra></extra>'
        ))

    # 레이아웃 설정
    fig.update_layout(
        title=f"Spike Detection - Keyword Proportion Over Time (Window: {window}M, Threshold: {z_threshold}σ)",
        xaxis_title="Month",
        yaxis_title="비율 (%) - 월별 전체 보고 대비",
        hovermode='x unified',
        height=600,
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02
        ),
        margin=dict(l=50, r=150, t=80, b=50)
    )

    # 기준 월 강조 (shapes를 사용하여 직접 그리기)
    if len(ts_df) > 0:
        # x축에서 as_of_month의 인덱스 찾기
        all_months = sorted(ts_df["month"].unique().to_list())
        if as_of_month in all_months:
            fig.add_shape(
                type="line",
                x0=as_of_month,
                x1=as_of_month,
                y0=0,
                y1=1,
                yref="paper",
                line=dict(color="red", width=2, dash="dash")
            )
            # 주석 추가
            fig.add_annotation(
                x=as_of_month,
                y=1,
                yref="paper",
                text="Analysis Month",
                showarrow=False,
                yshift=10,
                font=dict(color="red", size=10)
            )

    return fig


def prepare_spike_table(spike_df: pl.DataFrame) -> pl.DataFrame:
    """
    스파이크 테이블 표시용 데이터 준비

    Args:
        spike_df: 스파이크 탐지 결과 DataFrame

    Returns:
        표시용 DataFrame
    """
    display_df = spike_df.select([
        pl.col("keyword").alias("키워드"),
        pl.col("C_recent").alias("최근 보고수"),
        pl.col("C_base").alias("기준 보고수"),
        pl.col("ratio").alias("비율"),
        pl.col("is_spike").alias("Spike (Ratio)"),
        pl.col("is_spike_z").alias("Spike (Z-score)"),
        pl.col("is_spike_p").alias("Spike (Poisson)"),
        pl.col("n_methods").alias("스파이크 방법 수"),
        pl.col("pattern").alias("패턴"),
    ])

    # 패턴에 이모지 추가
    display_df = display_df.with_columns(
        pl.when(pl.col("패턴") == "severe").then(pl.lit("🔴 Severe"))
        .when(pl.col("패턴") == "alert").then(pl.lit("🟠 Alert"))
        .when(pl.col("패턴") == "attention").then(pl.lit("🟡 Attention"))
        .otherwise(pl.lit("🟢 General"))
        .alias("패턴")
    )

    return display_df