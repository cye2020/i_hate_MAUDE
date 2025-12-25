# overview_tab.py
import pandas as pd
import polars as pl
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from utils.filter_manager import create_sidebar

# overview_tab.py
def show(filters=None, lf: pl.LazyFrame = None):
    st.title("📊 Overview")

    # 필터에서 segment 값 가져오기 (None이면 전체)
    segment = filters.get("segment", None)

    # 세션 스테이트 초기화 (브러시 선택된 날짜 범위 저장)
    if 'selected_date_range' not in st.session_state:
        st.session_state.selected_date_range = None

    # Big Number 표시 (4개)
    big_numbers = calculate_big_numbers(
        lf,
        start=st.session_state.selected_date_range[0] if st.session_state.selected_date_range else None,
        end=st.session_state.selected_date_range[1] if st.session_state.selected_date_range else None,
        segment=segment
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="📁 총 보고서 수",
            value=f"{big_numbers['total_reports']:,}건"
        )

    with col2:
        st.metric(
            label="⚠️ 중대 피해 발생률",
            value=f"{big_numbers['severe_harm_rate']:.1f}%"
        )

    with col3:
        st.metric(
            label="🔧 제조사 결함 확인률",
            value=f"{big_numbers['defect_confirmed_rate']:.1f}%"
        )

    with col4:
        st.metric(
            label="⏱️ 평균 처리 기간",
            value=f"{big_numbers['avg_processing_days']:.0f}일"
        )

    st.markdown("---")

    # 차트 그리기
    plot_stacked_area_chart(lf, segment=segment)

def calculate_big_numbers(
    data: pl.LazyFrame,
    start: str = None,
    end: str = None,
    segment: str = None,
) -> dict:
    """Big Number 4개 계산

    Args:
        data: LazyFrame 데이터
        start: 시작 날짜 (브러시 선택 시)
        end: 종료 날짜 (브러시 선택 시)
        segment: 세그먼트 컬럼명 (현재는 사용 안함, 차트만 segment 적용)

    Returns:
        {
            'total_reports': 총 보고서 수,
            'severe_harm_rate': 중대 피해 발생률 (%),
            'defect_confirmed_rate': 제조사 결함 확인률 (%),
            'avg_processing_days': 평균 처리 기간 (일)
        }
    """
    # 날짜 필터링
    filtered_data = data
    if start and end:
        filtered_data = filtered_data.filter(
            (pl.col("date_received") >= start) & (pl.col("date_received") <= end)
        )

    # 집계
    df = filtered_data.select([
        pl.len().alias("total"),
        # 중대 피해 (Serious Injury + Death)
        pl.when(pl.col("patient_harm").is_in(["Serious Injury", "Death"]))
          .then(1).otherwise(0).sum().alias("severe_harm_count"),
        # 결함 확인
        pl.when(pl.col("defect_confirmed") == True)
          .then(1).otherwise(0).sum().alias("defect_confirmed_count"),
        # 평균 처리 기간 (date_received - date_occurred)
        (pl.col("date_received") - pl.col("date_occurred"))
          .dt.total_days()
          .mean()
          .alias("avg_processing_days"),
    ]).collect()

    total = df["total"][0]
    severe_harm = df["severe_harm_count"][0]
    defect_confirmed = df["defect_confirmed_count"][0]
    avg_days = df["avg_processing_days"][0] if df["avg_processing_days"][0] is not None else 0.0

    return {
        "total_reports": total,
        "severe_harm_rate": (severe_harm / total * 100) if total > 0 else 0.0,
        "defect_confirmed_rate": (defect_confirmed / total * 100) if total > 0 else 0.0,
        "avg_processing_days": avg_days,
    }

# 브러시 차트
def plot_stacked_area_chart(
        data: pl.LazyFrame,
        start: str = None,
        end: str = None,
        segment: str = None,
        top_n: int = 5
    ):
    """Report Count 시각화 (브러시 차트)

    Args:
        data: LazyFrame 데이터
        start: 시작 날짜 (예: "2024-01-01"), None이면 전체 기간
        end: 종료 날짜 (예: "2024-12-31"), None이면 전체 기간
        segment: 세그먼트 컬럼명 (예: "manufacturer_name", "device_type"), None이면 전체 집계
        top_n: segment별 상위 N개만 표시 (default=5)
    """

    # 1. 날짜 필터링
    filtered_data = data
    if start and end:
        # date_received 컬럼이 있다고 가정
        filtered_data = filtered_data.filter(
            (pl.col("date_received") >= start) & (pl.col("date_received") <= end)
        )

    # 2. 집계 수준에 따라 count
    if segment is None:
        # 전체 데이터 집계 (날짜별)
        agg_data = (
            filtered_data
            .group_by(pl.col("date_received").dt.truncate("1mo").alias("date"))
            .agg(pl.len().alias("count"))
            .sort("date")
            .collect()
        )
    else:
        # segment별 집계 (top_n만)
        # 먼저 segment별 전체 count를 구해서 top_n 추출
        top_segments = (
            filtered_data
            .group_by(segment)
            .agg(pl.len().alias("total_count"))
            .sort("total_count", descending=True)
            .limit(top_n)
            .select(segment)
            .collect()
        )

        top_segment_list = top_segments[segment].to_list()

        # top_n segment만 필터링 후 날짜별 집계
        agg_data = (
            filtered_data
            .filter(pl.col(segment).is_in(top_segment_list))
            .group_by(
                pl.col("date_received").dt.truncate("1mo").alias("date"),
                segment
            )
            .agg(pl.len().alias("count"))
            .sort("date", segment)
            .collect()
        )

    # 3. 시각화 (메인 차트 1개만 + rangeslider)
    st.subheader("📊 Report Count Over Time")

    fig = go.Figure()

    if segment is None:
        # 전체 집계: 단순 라인 차트
        fig.add_trace(
            go.Scatter(
                x=agg_data["date"],
                y=agg_data["count"],
                name="Reports",
                line=dict(color='#1f77b4', width=2),
                mode='lines+markers',
                fill='tozeroy',
                fillcolor='rgba(31, 119, 180, 0.2)'
            )
        )
    else:
        # segment별: 스택 차트
        for seg_value in top_segment_list:
            seg_data = agg_data.filter(pl.col(segment) == seg_value)

            fig.add_trace(
                go.Scatter(
                    x=seg_data["date"],
                    y=seg_data["count"],
                    name=str(seg_value),
                    mode='lines',
                    stackgroup='one',
                    line=dict(width=0.5)
                )
            )

    # 레이아웃 업데이트 (rangeslider 포함)
    fig.update_layout(
        height=600,
        hovermode='x unified',
        margin=dict(l=50, r=20, t=40, b=80),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(128, 128, 128, 0.2)',
            rangeslider=dict(
                visible=True,
                thickness=0.15
            ),
            # 년월 단위로 스냅
            dtick="M1",
            tickformat="%Y-%m"
        ),
        yaxis=dict(
            title="Report Count"
        )
    )

    # rangeslider 선택 이벤트 캡처
    event = st.plotly_chart(fig, width='stretch', on_select='rerun', key='overview_chart')

    # 디버그: 선택된 범위 출력
    st.write("### 디버그: Plotly Event")
    st.write("event:", event)

    if event and 'selection' in event:
        st.write("selection:", event['selection'])

    if event and 'range' in event:
        st.write("range:", event['range'])

    return agg_data
