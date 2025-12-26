# overview_tab.py
import polars as pl
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.analysis import calculate_big_numbers
from utils.constants import ColumnNames, PatientHarmLevels

# overview_tab.py
def show(filters=None, lf: pl.LazyFrame = None):
    st.title("📊 Overview")

    # 필터에서 segment 값 가져오기 (None이면 전체)
    segment = filters.get("segment", None)

    # 날짜 범위 가져오기 (month_range_picker에서)
    date_range = filters.get("date_range", None)
    start_date = None
    end_date = None

    if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range

    # 세션 스테이트 초기화 (브러시 선택된 날짜 범위 저장)
    if 'selected_date_range' not in st.session_state:
        st.session_state.selected_date_range = None

    # Big Number 표시 (4개) - 선택된 기간의 최신 한 달 vs 전월 비교
    big_numbers = calculate_big_numbers(
        _data=lf,
        segment=segment,
        start_date=start_date,
        end_date=end_date
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="📁 총 보고서 수",
            value=f"{big_numbers['total_reports']:,}건",
            delta=f"{big_numbers['total_reports_delta']:+.1f}%" if big_numbers['total_reports_delta'] is not None else None
        )

    with col3:
        st.metric(
            label="⚠️ 중대 피해 발생률",
            value=f"{big_numbers['severe_harm_rate']:.1f}%",
            delta=f"{big_numbers['severe_harm_rate_delta']:+.1f}%p" if big_numbers['severe_harm_rate_delta'] is not None else None
        )

    with col4:
        st.metric(
            label="🔧 제조사 결함 확정률",
            value=f"{big_numbers['defect_confirmed_rate']:.1f}%",
            delta=f"{big_numbers['defect_confirmed_rate_delta']:+.1f}%p" if big_numbers['defect_confirmed_rate_delta'] is not None else None
        )

    with col2:
        # delta에 이전 기간의 가장 치명적인 defect type 표시
        prev_defect_info = f"이전: {big_numbers['prev_most_critical_defect_type']} ({big_numbers['prev_most_critical_defect_rate']:.1f}%)"
        st.metric(
            label="🔥 가장 치명적인 Defect Type",
            value=big_numbers['most_critical_defect_type'],
            delta=prev_defect_info,
            delta_arrow='off',
            delta_color="off"  # delta를 회색으로 표시 (증감이 아니라 정보)
        )

    st.markdown("---")

    # 차트 그리기 (날짜 범위 적용)
    start_str = start_date.strftime("%Y-%m-%d") if start_date else None
    end_str = end_date.strftime("%Y-%m-%d") if end_date else None
    plot_stacked_area_chart(lf, start=start_str, end=end_str, segment=segment)


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
        # 문자열을 datetime으로 변환하여 비교
        from datetime import datetime
        start_dt = datetime.strptime(start, "%Y-%m-%d") if isinstance(start, str) else start
        end_dt = datetime.strptime(end, "%Y-%m-%d") if isinstance(end, str) else end

        filtered_data = filtered_data.filter(
            (pl.col("date_received") >= start_dt) & (pl.col("date_received") <= end_dt)
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

    # # 디버그: 선택된 범위 출력
    # st.write("### 디버그: Plotly Event")
    # st.write("event:", event)

    # if event and 'selection' in event:
    #     st.write("selection:", event['selection'])

    # if event and 'range' in event:
    #     st.write("range:", event['range'])

    return agg_data
