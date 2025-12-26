# eda_tab.py (리팩토링 버전)
import streamlit as st
import polars as pl
import pandas as pd

# utils 함수 import
from utils.constants import ColumnNames, Defaults
from utils.data_utils import get_year_month_expr, get_window_dates
from utils.filter_helpers import (
    get_available_filters,
    get_manufacturers_by_dates,
    get_products_by_manufacturers,
    get_available_defect_types
)
from utils.analysis import (
    get_filtered_products,
    get_monthly_counts,
    analyze_manufacturer_defects,
    analyze_defect_components,
    calculate_cfr_by_device
)
from utils.analysis_cluster import (
    get_available_clusters,
    cluster_keyword_unpack,
    get_event_type_summary
)


def show(filters=None, lf: pl.LazyFrame = None):
    """EDA 탭 메인 함수

    Args:
        filters: 사이드바 필터 값
        lf: LazyFrame 데이터 (Home.py에서 전달)
    """
    st.title("📈 Detailed Analysis")

    # 필터 값 사용
    selected_date = filters.get("date")
    categories = filters.get("categories", [])
    confidence_interval = filters.get("confidence_interval", 0.95)

    # 데이터 확인
    if lf is None:
        st.error("데이터를 로드할 수 없습니다.")
        return

    try:
        # 년-월 컬럼 생성 표현식을 한 번만 계산 (재사용)
        date_col = ColumnNames.DATE_RECEIVED
        year_month_expr = get_year_month_expr(lf, date_col)

        # 사용 가능한 필터 옵션 가져오기
        with st.spinner("필터 옵션 로딩 중..."):
            available_dates, available_manufacturers, available_products = get_available_filters(
                lf,
                date_col=date_col,
                _year_month_expr=year_month_expr
            )

        if len(available_dates) == 0:
            st.warning("사용 가능한 날짜 데이터가 없습니다. 데이터 파일과 날짜 컬럼을 확인해주세요.")
            st.stop()

        # 필터 UI 렌더링
        selected_dates, selected_manufacturers, selected_products, top_n = render_filter_ui(
            available_dates,
            available_manufacturers,
            available_products,
            lf,
            date_col,
            year_month_expr
        )

        # 월별 보고서 수 그래프
        render_monthly_reports_chart(
            lf,
            date_col,
            selected_dates,
            selected_manufacturers,
            selected_products,
            top_n,
            year_month_expr
        )

        # 제조사 - 모델별 결함 분석
        st.markdown("---")
        render_defect_analysis(
            lf,
            date_col,
            selected_dates,
            selected_manufacturers,
            selected_products,
            year_month_expr
        )

        # 문제 부품 분석
        st.markdown("---")
        render_component_analysis(
            lf,
            date_col,
            selected_dates,
            selected_manufacturers,
            selected_products,
            year_month_expr
        )

        # 기기별 치명률(CFR) 분석
        st.markdown("---")
        render_cfr_analysis(
            lf,
            date_col,
            selected_dates,
            selected_manufacturers,
            selected_products,
            year_month_expr
        )

        # defect type별 상위 문제 & 사건 유형별 분포
        st.markdown("---")
        render_cluster_and_event_analysis(
            lf,
            date_col,
            selected_dates,
            selected_manufacturers,
            selected_products,
            year_month_expr
        )

    except Exception as e:
        st.error(f"데이터 분석 중 오류가 발생했습니다: {str(e)}")
        st.exception(e)


def render_filter_ui(
    available_dates,
    available_manufacturers,
    available_products,
    lf,
    date_col,
    year_month_expr
):
    """필터 UI 렌더링"""
    col1, col2, col3 = st.columns(3)

    # 년-월 선택
    with col1:
        prev_selected_dates = st.session_state.get('prev_selected_dates', [])
        sidebar_year_month = st.session_state.get('selected_year_month', None)
        sidebar_window = st.session_state.get('selected_window', 1)

        # 기본값 설정
        default_dates = [sidebar_year_month] if sidebar_year_month and sidebar_year_month in available_dates else []
        if not default_dates and available_dates:
            default_dates = [available_dates[0]]

        if prev_selected_dates:
            valid_prev_dates = [d for d in prev_selected_dates if d in available_dates]
            if valid_prev_dates:
                default_dates = valid_prev_dates

        # 윈도우 기반 자동 선택
        use_window = st.checkbox(
            "윈도우 기간 자동 선택 (최근 k개월 + 직전 k개월)",
            value=st.session_state.get('use_window', True if sidebar_year_month else False),
            key='use_window_checkbox'
        )
        st.session_state.use_window = use_window

        if use_window and sidebar_year_month:
            recent_months, base_months = get_window_dates(
                available_dates,
                sidebar_window,
                sidebar_year_month
            )
            window_dates = list(set(recent_months + base_months))
            if prev_selected_dates:
                valid_window_dates = [d for d in prev_selected_dates if d in available_dates]
                final_default = valid_window_dates if valid_window_dates else window_dates
            else:
                final_default = window_dates

            selected_dates = st.multiselect(
                "년-월 선택 (윈도우 기간 자동 선택됨)",
                options=available_dates,
                default=final_default,
                key='dates_multiselect'
            )
        else:
            selected_dates = st.multiselect(
                "년-월 선택 (사이드바 값이 기본 적용됨)",
                options=available_dates,
                default=default_dates,
                key='dates_multiselect'
            )

        if selected_dates:
            st.session_state.prev_selected_dates = selected_dates
        elif 'prev_selected_dates' in st.session_state and not selected_dates:
            del st.session_state.prev_selected_dates

    # 제조사 선택
    with col2:
        if selected_dates:
            filtered_manufacturers = get_manufacturers_by_dates(
                lf,
                selected_dates,
                date_col=date_col,
                _year_month_expr=year_month_expr
            )
            prev_selected = st.session_state.get('prev_selected_manufacturers', [])
            valid_selected_manufacturers = [m for m in prev_selected if m in filtered_manufacturers]
            manufacturer_options = filtered_manufacturers
            default_manufacturers = valid_selected_manufacturers
        else:
            manufacturer_options = available_manufacturers
            default_manufacturers = []
            if 'prev_selected_manufacturers' in st.session_state:
                del st.session_state.prev_selected_manufacturers

        help_text = (
            f"선택된 년-월({len(selected_dates)}개)에 존재하는 제조사만 표시됩니다"
            if selected_dates
            else "제조사를 선택하면 해당 제조사의 제품군만 표시됩니다"
        )

        selected_manufacturers = st.multiselect(
            "제조사 선택 (선택 안 함 = 전체)",
            options=manufacturer_options,
            default=default_manufacturers,
            help=help_text,
            key='manufacturers_multiselect'
        )

        if selected_manufacturers:
            st.session_state.prev_selected_manufacturers = selected_manufacturers
        else:
            if 'prev_selected_manufacturers' in st.session_state:
                del st.session_state.prev_selected_manufacturers

    # 제품군 선택
    with col3:
        if selected_manufacturers:
            filtered_products = get_products_by_manufacturers(
                lf,
                selected_manufacturers,
                manufacturer_col=ColumnNames.MANUFACTURER,
                product_col=ColumnNames.PRODUCT_CODE
            )
            prev_selected = st.session_state.get('prev_selected_products', [])
            valid_selected_products = [p for p in prev_selected if p in filtered_products]
            product_options = filtered_products
            default_products = valid_selected_products
        else:
            product_options = available_products
            default_products = []
            if 'prev_selected_products' in st.session_state:
                del st.session_state.prev_selected_products

        help_text = (
            f"선택된 년-월({len(selected_dates)}개)에 존재하는 제품군만 표시됩니다"
            if selected_dates
            else "제품군을 선택하면 해당 제품군의 보고 건수만 표시됩니다"
        )

        selected_products = st.multiselect(
            "제품군 선택 (선택 안 함 = 전체)",
            options=product_options,
            default=default_products,
            help=help_text,
            key='products_multiselect'
        )

        if selected_products:
            st.session_state.prev_selected_products = selected_products
        elif 'prev_selected_products' in st.session_state and not selected_products:
            del st.session_state.prev_selected_products

    # 상위 N개 선택
    default_top_n = st.session_state.get('top_n', Defaults.TOP_N)
    top_n = st.number_input(
        "상위 N개 표시",
        min_value=1,
        max_value=100,
        value=default_top_n,
        step=1,
        key='top_n_input'
    )
    st.session_state.top_n = top_n

    return selected_dates, selected_manufacturers, selected_products, top_n


def render_monthly_reports_chart(
    lf,
    date_col,
    selected_dates,
    selected_manufacturers,
    selected_products,
    top_n,
    year_month_expr
):
    """월별 보고서 수 차트 렌더링"""
    st.subheader("📊 월별 보고서 수")

    with st.spinner("데이터 분석 중..."):
        # 데이터 집계
        result_df = get_filtered_products(
            lf,
            date_col=date_col,
            selected_dates=selected_dates if selected_dates else None,
            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
            selected_products=selected_products if selected_products else None,
            top_n=top_n,
            _year_month_expr=year_month_expr
        )

        if len(result_df) > 0:
            # 결과 테이블
            display_df = result_df.to_pandas().copy()
            display_df.insert(0, "순위", range(1, len(display_df) + 1))
            display_df = display_df[["순위", "manufacturer_product", "total_count"]]
            display_df.columns = ["순위", "제조사-제품군", "보고 건수"]

            # 월별 데이터
            monthly_df = get_monthly_counts(
                lf,
                date_col=date_col,
                selected_dates=selected_dates if selected_dates else None,
                selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                selected_products=selected_products if selected_products else None,
                _year_month_expr=year_month_expr
            )

            if len(monthly_df) > 0:
                monthly_pandas = monthly_df.to_pandas()
                top_combinations = display_df.head(top_n)["제조사-제품군"].tolist()
                chart_data = monthly_pandas[
                    monthly_pandas["manufacturer_product"].isin(top_combinations)
                ].copy()

                if selected_dates:
                    # 막대 차트
                    bar_chart_data = display_df.head(top_n).set_index("제조사-제품군")[["보고 건수"]]
                    st.bar_chart(bar_chart_data, width='stretch')
                else:
                    # 선 그래프
                    pivot_df = chart_data.pivot_table(
                        index="year_month",
                        columns="manufacturer_product",
                        values="total_count",
                        aggfunc='first',
                        fill_value=0
                    )
                    pivot_df = pivot_df.sort_index()
                    st.line_chart(pivot_df, width='stretch')

            # 테이블 표시
            st.dataframe(display_df, width='stretch', hide_index=True)
        else:
            st.info("선택한 조건에 해당하는 데이터가 없습니다.")


def render_defect_analysis(
    lf,
    date_col,
    selected_dates,
    selected_manufacturers,
    selected_products,
    year_month_expr
):
    """제조사-모델별 결함 분석 렌더링"""
    st.subheader("🔧 제조사 - 모델별 결함")

    if not selected_dates:
        st.info("결함 분석을 위해 년-월을 선택해주세요.")
        return

    with st.spinner("결함 분석 중..."):
        defect_df = analyze_manufacturer_defects(
            lf,
            date_col=date_col,
            selected_dates=selected_dates,
            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
            selected_products=selected_products if selected_products else None,
            _year_month_expr=year_month_expr
        )

    if len(defect_df) > 0:
        display_df = defect_df.to_pandas()
        unique_manufacturers = display_df["manufacturer_product"].unique()

        if len(unique_manufacturers) > 0:
            view_mode = st.radio(
                "보기 모드",
                options=["단일 제조사-제품군", "전체 비교"],
                horizontal=True
            )

            if view_mode == "단일 제조사-제품군":
                selected_manufacturer = st.selectbox(
                    "제조사-제품군 선택",
                    options=unique_manufacturers,
                    index=0
                )

                mfr_data = display_df[
                    display_df["manufacturer_product"] == selected_manufacturer
                ].copy()

                if len(mfr_data) > 0:
                    chart_data = pd.DataFrame({
                        "결함 유형": mfr_data[ColumnNames.DEFECT_TYPE],
                        "건수": mfr_data["count"],
                        "비율(%)": mfr_data["percentage"]
                    }).sort_values("건수", ascending=False)

                    st.bar_chart(
                        chart_data.set_index("결함 유형")[["비율(%)"]],
                        width='stretch'
                    )

                    st.dataframe(
                        chart_data[["결함 유형", "건수", "비율(%)"]],
                        width='stretch',
                        hide_index=True
                    )
                else:
                    st.info(f"{selected_manufacturer}에 대한 결함 데이터가 없습니다.")
            else:
                # 전체 비교
                pivot_df = display_df.pivot_table(
                    index=ColumnNames.DEFECT_TYPE,
                    columns="manufacturer_product",
                    values="percentage",
                    aggfunc='first',
                    fill_value=0
                )

                st.bar_chart(pivot_df, width='stretch')

                st.dataframe(
                    display_df[[
                        "manufacturer_product",
                        ColumnNames.DEFECT_TYPE,
                        "count",
                        "percentage"
                    ]].sort_values(["manufacturer_product", "count"], ascending=[True, False])
                    .rename(columns={
                        "manufacturer_product": "제조사-제품군",
                        ColumnNames.DEFECT_TYPE: "결함 유형",
                        "count": "건수",
                        "percentage": "비율(%)"
                    }),
                    width='stretch',
                    hide_index=True
                )
        else:
            st.info("결함 데이터가 없습니다.")
    else:
        st.info("선택한 조건에 해당하는 결함 데이터가 없습니다.")


def render_component_analysis(
    lf,
    date_col,
    selected_dates,
    selected_manufacturers,
    selected_products,
    year_month_expr
):
    """문제 부품 분석 렌더링"""
    st.subheader("🔩 문제 부품 분석")

    if not selected_dates:
        st.info("문제 부품 분석을 위해 년-월을 선택해주세요.")
        return

    try:
        with st.spinner("결함 유형 목록 로딩 중..."):
            available_defect_types = get_available_defect_types(
                lf,
                date_col=date_col,
                selected_dates=selected_dates,
                selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                selected_products=selected_products if selected_products else None,
                _year_month_expr=year_month_expr
            )

        if len(available_defect_types) > 0:
            col1, col2 = st.columns([2, 1])

            with col1:
                prev_selected_defect_type = st.session_state.get('prev_selected_defect_type', None)
                default_index = 0
                if prev_selected_defect_type and prev_selected_defect_type in available_defect_types:
                    default_index = available_defect_types.index(prev_selected_defect_type)

                selected_defect_type = st.selectbox(
                    "결함 유형 선택",
                    options=available_defect_types,
                    index=default_index,
                    help="분석할 결함 유형을 선택하세요",
                    key='defect_type_selectbox'
                )
                st.session_state.prev_selected_defect_type = selected_defect_type

            with col2:
                default_top_n_components = st.session_state.get('top_n_components', Defaults.TOP_N)
                top_n_components = st.number_input(
                    "상위 N개 표시",
                    min_value=1,
                    max_value=50,
                    value=default_top_n_components,
                    step=1,
                    key='top_n_components_input'
                )
                st.session_state.top_n_components = top_n_components

            if selected_defect_type:
                with st.spinner("문제 부품 분석 중..."):
                    component_df = analyze_defect_components(
                        lf,
                        defect_type=selected_defect_type,
                        date_col=date_col,
                        selected_dates=selected_dates,
                        selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                        selected_products=selected_products if selected_products else None,
                        top_n=top_n_components,
                        _year_month_expr=year_month_expr
                    )

                if component_df is not None and len(component_df) > 0:
                    display_df = component_df.to_pandas().copy()

                    display_df[ColumnNames.PROBLEM_COMPONENTS] = display_df[ColumnNames.PROBLEM_COMPONENTS].apply(
                        lambda x: str(x) if x is not None else "(NULL)"
                    )

                    display_df.insert(0, "순위", range(1, len(display_df) + 1))
                    display_df = display_df[["순위", ColumnNames.PROBLEM_COMPONENTS, "count", "percentage"]]
                    display_df.columns = ["순위", "문제 부품", "건수", "비율(%)"]

                    st.dataframe(
                        display_df,
                        width='stretch',
                        hide_index=True
                    )
                else:
                    st.info(f"'{selected_defect_type}' 결함 유형에 대한 문제 부품 데이터가 없습니다.")
        else:
            st.info("선택한 조건에 해당하는 결함 유형이 없습니다.")

    except Exception as e:
        st.error(f"문제 부품 분석 중 오류가 발생했습니다: {str(e)}")
        st.exception(e)


def render_cfr_analysis(
    lf,
    date_col,
    selected_dates,
    selected_manufacturers,
    selected_products,
    year_month_expr
):
    """기기별 치명률(CFR) 분석 렌더링"""
    st.subheader("💀 기기별 치명률(CFR) 분석")

    try:
        col1, col2 = st.columns([2, 1])

        with col1:
            default_top_n_cfr = st.session_state.get('top_n_cfr', 20)
            top_n_cfr = st.number_input(
                "상위 N개 표시 (CFR 분석)",
                min_value=1,
                max_value=100,
                value=default_top_n_cfr,
                step=1,
                help="None을 선택하면 전체 결과가 표시됩니다",
                key='top_n_cfr_input'
            )
            st.session_state.top_n_cfr = top_n_cfr

        with col2:
            default_min_cases = st.session_state.get('min_cases', Defaults.MIN_CASES)
            min_cases = st.number_input(
                "최소 보고 건수",
                min_value=1,
                max_value=1000,
                value=default_min_cases,
                step=1,
                help="이 값보다 적은 건수의 기기는 제외됩니다 (통계적 신뢰도 확보)",
                key='min_cases_input'
            )
            st.session_state.min_cases = min_cases

        with st.spinner("기기별 치명률 분석 중..."):
            cfr_result = calculate_cfr_by_device(
                lf,
                date_col=date_col,
                selected_dates=selected_dates if selected_dates else None,
                selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                selected_products=selected_products if selected_products else None,
                top_n=top_n_cfr if top_n_cfr else None,
                min_cases=min_cases,
                _year_month_expr=year_month_expr
            )

        if len(cfr_result) > 0:
            display_df = cfr_result.to_pandas().copy()

            display_df.insert(0, "순위", range(1, len(display_df) + 1))
            display_df = display_df[[
                "순위", "manufacturer_product", "total_cases",
                "death_count", "injury_count", "malfunction_count",
                "cfr", "injury_rate", "malfunction_rate"
            ]]
            display_df.columns = [
                "순위", "제조사-제품군", "총 건수",
                "사망", "부상", "오작동",
                "CFR(%)", "부상률(%)", "오작동률(%)"
            ]

            st.dataframe(display_df, width='stretch', hide_index=True)

            # 요약 통계
            st.markdown("**요약 통계**")
            summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)

            with summary_col1:
                st.metric("분석 기기 수", f"{len(display_df):,}개")

            with summary_col2:
                avg_cfr = display_df["CFR(%)"].mean()
                st.metric("평균 CFR", f"{avg_cfr:.2f}%")

            with summary_col3:
                max_cfr = display_df["CFR(%)"].max()
                st.metric("최대 CFR", f"{max_cfr:.2f}%")

            with summary_col4:
                median_cfr = display_df["CFR(%)"].median()
                st.metric("CFR 중앙값", f"{median_cfr:.2f}%")
        else:
            st.info(f"선택한 조건에 해당하는 데이터가 없습니다. (최소 {min_cases}건 이상의 보고 건수 필요)")

    except Exception as e:
        st.error(f"기기별 치명률 분석 중 오류가 발생했습니다: {str(e)}")
        st.exception(e)


def render_cluster_and_event_analysis(
    lf,
    date_col,
    selected_dates,
    selected_manufacturers,
    selected_products,
    year_month_expr
):
    """defect type별 상위 문제 & 사건 유형별 분포 렌더링"""
    import plotly.graph_objects as go
    import streamlit.components.v1 as components
    import html

    st.subheader("📊 defect type별 상위 문제 & 사건 유형 분포")

    try:
        # 사용 가능한 defect type 가져오기
        with st.spinner("defect type 목록 로딩 중..."):
            available_clusters = get_available_clusters(
                lf,
                cluster_col=ColumnNames.DEFECT_TYPE,
                date_col=date_col,
                selected_dates=selected_dates if selected_dates else None,
                selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                selected_products=selected_products if selected_products else None,
                _year_month_expr=year_month_expr
            )

        if len(available_clusters) > 0:
            # 좌우 레이아웃
            event_col, cluster_col = st.columns([1, 1])

            # 우측: defect type별 상위 문제
            with cluster_col:
                st.markdown("### defect type별 상위 문제")

                # 이전에 선택한 defect type 가져오기
                prev_selected_cluster = st.session_state.get('prev_selected_cluster', None)
                default_index = 0
                if prev_selected_cluster and prev_selected_cluster in available_clusters:
                    default_index = available_clusters.index(prev_selected_cluster)

                selected_cluster = st.selectbox(
                    "카테고리 선택",
                    options=available_clusters,
                    index=default_index,
                    help="분석할 defect type를 선택하세요",
                    key='cluster_selectbox',
                    label_visibility="collapsed"
                )
                st.session_state.prev_selected_cluster = selected_cluster

                # 상위 N개 설정 (기본값 10개)
                top_n_cluster = 10

                # defect type별 상위 문제 분석 실행
                if selected_cluster:
                    with st.spinner("defect type별 상위 문제 분석 중..."):
                        cluster_result = cluster_keyword_unpack(
                            lf,
                            col_name=ColumnNames.PROBLEM_COMPONENTS,
                            cluster_col=ColumnNames.DEFECT_TYPE,
                            date_col=date_col,
                            selected_dates=selected_dates if selected_dates else None,
                            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                            selected_products=selected_products if selected_products else None,
                            top_n=top_n_cluster,
                            _year_month_expr=year_month_expr
                        )

                    # 선택된 defect type의 데이터만 필터링
                    cluster_data = cluster_result.filter(
                        pl.col(ColumnNames.DEFECT_TYPE) == selected_cluster
                    )

                    if len(cluster_data) > 0:
                        # 결과를 pandas DataFrame으로 변환
                        display_df = cluster_data.to_pandas().copy()

                        # problem_components를 문자열로 변환
                        display_df[ColumnNames.PROBLEM_COMPONENTS] = display_df[ColumnNames.PROBLEM_COMPONENTS].apply(
                            lambda x: str(x) if x is not None else "(NULL)"
                        )

                        # 정렬 (count 내림차순)
                        display_df = display_df.sort_values('count', ascending=False).reset_index(drop=True)

                        # HTML/CSS를 사용한 부드럽고 둥근 막대 차트
                        max_visible_items = 10  # 화면에 보이는 항목 수
                        item_height = 55  # 각 항목의 높이
                        container_height = max_visible_items * item_height  # 스크롤 컨테이너 높이

                        # 최대 비율 계산 (막대 길이 계산용)
                        max_ratio = display_df['ratio'].max() if len(display_df) > 0 else 100

                        # HTML/CSS 스타일과 컨테이너 (f-string 사용)
                        bar_height = item_height - 10
                        html_content = f"""
                        <style>
                            .cluster-bar-container {{
                                height: {container_height}px;
                                overflow-y: auto;
                                overflow-x: hidden;
                                padding: 10px 5px;
                                scroll-behavior: smooth;
                            }}
                            .cluster-bar-container::-webkit-scrollbar {{
                                width: 8px;
                            }}
                            .cluster-bar-container::-webkit-scrollbar-track {{
                                background: #f1f1f1;
                                border-radius: 10px;
                            }}
                            .cluster-bar-container::-webkit-scrollbar-thumb {{
                                background: #888;
                                border-radius: 10px;
                            }}
                            .cluster-bar-container::-webkit-scrollbar-thumb:hover {{
                                background: #555;
                            }}
                            .cluster-item {{
                                display: flex;
                                align-items: center;
                                gap: 10px;
                                margin-bottom: 12px;
                                padding: 8px 0;
                                transition: transform 0.2s ease;
                            }}
                            .cluster-item:hover {{
                                transform: translateX(3px);
                            }}
                            .component-name {{
                                width: 140px;
                                font-size: 14px;
                                color: #374151;
                                flex-shrink: 0;
                                text-align: left;
                                font-weight: 500;
                                overflow: hidden;
                                text-overflow: ellipsis;
                                white-space: nowrap;
                            }}
                            .bar-wrapper {{
                                flex: 1;
                                position: relative;
                                height: {bar_height}px;
                                background-color: #F3F4F6;
                                border-radius: 20px;
                                overflow: hidden;
                            }}
                            .bar-fill {{
                                position: absolute;
                                left: 0;
                                top: 0;
                                height: 100%;
                                background: linear-gradient(90deg, #3B82F6 0%, #2563EB 100%);
                                border-radius: 20px;
                                transition: width 0.3s ease;
                                box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);
                            }}
                            .bar-content {{
                                position: absolute;
                                top: 50%;
                                transform: translateY(-50%);
                                left: 15px;
                                font-size: 15px;
                                font-weight: 600;
                                color: white;
                                text-shadow: 0 1px 2px rgba(0,0,0,0.2);
                                z-index: 2;
                            }}
                            .bar-ratio {{
                                position: absolute;
                                top: 50%;
                                transform: translateY(-50%);
                                right: 15px;
                                font-size: 14px;
                                font-weight: 500;
                                color: #6B7280;
                                background-color: rgba(243, 244, 246, 0.95);
                                padding: 5px 10px;
                                border-radius: 12px;
                                z-index: 2;
                                backdrop-filter: blur(4px);
                            }}
                        </style>
                        <div class="cluster-bar-container">
                        """

                        for idx, row in display_df.iterrows():
                            component = row[ColumnNames.PROBLEM_COMPONENTS]
                            count = int(row['count'])
                            ratio = float(row['ratio'])
                            # 막대 길이는 비율에 비례 (최대 비율을 100%로 설정)
                            bar_width = (ratio / max_ratio) * 100 if max_ratio > 0 else 0

                            # 컴포넌트 이름이 너무 길면 자르기
                            display_component = component[:30] + "..." if len(component) > 30 else component

                            # HTML 이스케이프 처리
                            escaped_component = html.escape(str(component))
                            escaped_display = html.escape(str(display_component))

                            html_content += f"""
                            <div class="cluster-item">
                                <div class="component-name" title="{escaped_component}">{escaped_display}</div>
                                <div class="bar-wrapper">
                                    <div class="bar-fill" style="width: {bar_width}%;"></div>
                                    <span class="bar-content">{count:,}</span>
                                    <span class="bar-ratio">{ratio:.1f}%</span>
                                </div>
                            </div>
                            """

                        html_content += "</div>"

                        # HTML 렌더링 (components.html 사용)
                        components.html(html_content, height=container_height + 20, scrolling=True)
                    else:
                        st.info(f"'{selected_cluster}' defect type에 대한 문제 부품 데이터가 없습니다.")

            # 좌측: 사건 유형별 분포 파이 차트
            with event_col:
                st.markdown("### 사건 유형별 분포")

                with st.spinner("사건 유형 데이터 로딩 중..."):
                    event_summary = get_event_type_summary(
                        lf,
                        event_column=ColumnNames.EVENT_TYPE,
                        date_col=date_col,
                        selected_dates=selected_dates if selected_dates else None,
                        selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                        selected_products=selected_products if selected_products else None,
                        _year_month_expr=year_month_expr
                    )

                total_deaths = event_summary['total_deaths']
                total_injuries = event_summary['total_injuries']
                total_malfunctions = event_summary['total_malfunctions']
                total_all = event_summary['total_all']

                if total_all > 0:
                    # 파이 차트 데이터 준비
                    pie_data = pd.DataFrame({
                        '유형': ['사망', '부상', '오작동'],
                        '건수': [total_deaths, total_injuries, total_malfunctions],
                        '비율': [
                            (total_deaths / total_all * 100),
                            (total_injuries / total_all * 100),
                            (total_malfunctions / total_all * 100)
                        ]
                    })

                    # Plotly 파이 차트 생성
                    fig_pie = go.Figure(data=[go.Pie(
                        labels=pie_data['유형'],
                        values=pie_data['건수'],
                        hole=0.4,  # 도넛 차트 스타일
                        marker=dict(
                            colors=['#DC2626', '#F59E0B', '#3B82F6'],  # 빨강(사망), 주황(부상), 파랑(오작동)
                            line=dict(color='#FFFFFF', width=2)
                        ),
                        textinfo='label+percent+value',
                        texttemplate='%{label}<br>%{value:,}건<br>(%{percent})',
                        hovertemplate='<b>%{label}</b><br>건수: %{value:,}<br>비율: %{percent}<extra></extra>'
                    )])

                    fig_pie.update_layout(
                        showlegend=True,
                        legend=dict(
                            orientation="v",
                            yanchor="middle",
                            y=0.5,
                            xanchor="left",
                            x=1.05
                        ),
                        height=400,
                        margin=dict(l=20, r=20, t=20, b=20),
                        paper_bgcolor='white',
                        plot_bgcolor='white'
                    )

                    # 파이 차트 표시
                    st.plotly_chart(fig_pie, width='stretch', config={'displayModeBar': False})

                    # 요약 정보
                    st.markdown("**전체 요약**")
                    summary_col1, summary_col2, summary_col3 = st.columns(3)

                    with summary_col1:
                        st.metric("사망", f"{total_deaths:,}건")

                    with summary_col2:
                        st.metric("부상", f"{total_injuries:,}건")

                    with summary_col3:
                        st.metric("오작동", f"{total_malfunctions:,}건")
                else:
                    st.info("사건 데이터가 없습니다.")
        else:
            st.info("선택한 조건에 해당하는 defect type가 없습니다.")

    except Exception as e:
        st.error(f"defect type별 상위 문제 분석 중 오류가 발생했습니다: {str(e)}")
        st.exception(e)
