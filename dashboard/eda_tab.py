# eda_tab.py
import streamlit as st
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

@st.cache_data # 캐싱 
def load_data():
    """데이터 로드 함수 (캐싱)"""
    data_path = Path(__file__).parent.parent / 'data' / 'gold' / 'maude.parquet'
    if data_path.exists():
        return pl.scan_parquet(str(data_path))
    else:
        return None

def get_year_month_expr(_lf, date_col='date_received'):
    """
    년-월 컬럼 생성 표현식을 반환 (날짜 타입에 따라 자동 처리)
    
    Args:
        _lf: LazyFrame
        date_col: 날짜 컬럼명
    
    Returns:
        polars 표현식 (year_month 컬럼)
    """
    try:
        schema = _lf.collect_schema()
        date_dtype = None
        for name, dtype in schema.items():
            if name == date_col:
                date_dtype = dtype
                break
        
        if date_dtype == pl.Date:
            # 이미 Date 타입인 경우
            return (
                pl.col(date_col)
                .dt.strftime("%Y-%m")
                .alias("year_month")
            )
        else:
            # 문자열인 경우 (YYYYMMDD 형식)
            return (
                pl.col(date_col)
                .cast(pl.Utf8)
                .str.strptime(pl.Date, format="%Y%m%d", strict=False)
                .dt.strftime("%Y-%m")
                .alias("year_month")
            )
    except:
        # 기본값: 문자열로 가정
        return (
            pl.col(date_col)
            .cast(pl.Utf8)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .dt.strftime("%Y-%m")
            .alias("year_month")
        )

@st.cache_data 
def get_filtered_products(_lf, #함수 _lf파라미터 사용
                          manufacturer_col='manufacturer_name', 
                          product_col='product_code',
                          date_col='date_received',
                          selected_dates=None,
                          selected_manufacturers=None,
                          selected_products=None,
                          top_n=None,
                          _year_month_expr=None):
    """
    제조사-제품군 조합을 필터링하여 이상 사례 발생 수 집계
    
    Args:
        _lf: LazyFrame (언더스코어로 시작하여 캐싱에서 제외)
        manufacturer_col: 제조사 컬럼명
        product_col: 제품군(제품코드) 컬럼명
        date_col: 날짜 컬럼명 (기본: date_received)
        selected_dates: 선택된 년-월 리스트 (예: ['2024-01', '2024-02'])
        selected_manufacturers: 선택된 제조사 리스트
        selected_products: 선택된 제품군 리스트
        top_n: 상위 N개만 반환 (None이면 전체)
        _year_month_expr: 년-월 컬럼 생성 표현식 (재사용용, 언더스코어로 시작하여 캐싱에서 제외)
    
    Returns:
        필터링된 결과 DataFrame
    """
    
    # manufacturer_name과 product_code 조합 생성
    combo_expr = (
        pl.when(pl.col(manufacturer_col).is_not_null() & pl.col(product_col).is_not_null())
        .then(
            pl.col(manufacturer_col).cast(pl.Utf8)
            + pl.lit(" - ")
            + pl.col(product_col).cast(pl.Utf8)
        )
        .otherwise(pl.lit("(정보 없음)"))
        .alias("manufacturer_product")
    )
    
    # 년-월 컬럼 생성 표현식 (재사용 또는 새로 생성)
    year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
    
    # 기본 필터: null이 아닌 값들만
    filtered_lf = (
        _lf
        .with_columns([combo_expr, year_month_expr])
        .filter(
            pl.col(manufacturer_col).is_not_null() & 
            pl.col(product_col).is_not_null() &
            pl.col(date_col).is_not_null()
        )
    )
    
    # 날짜 필터 적용
    if selected_dates and len(selected_dates) > 0:
        filtered_lf = filtered_lf.filter(pl.col("year_month").is_in(selected_dates))
    
    # 제조사 필터 적용
    if selected_manufacturers and len(selected_manufacturers) > 0:
        filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
    
    # 제품군 필터 적용
    if selected_products and len(selected_products) > 0:
        filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))
    
    # 집계
    result = (
        filtered_lf
        .group_by("manufacturer_product")
        .agg(pl.len().alias("total_count"))
        .sort("total_count", descending=True)
    )
    
    # top_n 처리
    if top_n is not None:
        result = result.head(top_n)
    
    return result.collect()

@st.cache_data
def get_monthly_counts(_lf,
                       manufacturer_col='manufacturer_name', 
                       product_col='product_code',
                       date_col='date_received',
                       selected_dates=None,
                       selected_manufacturers=None,
                       selected_products=None,
                       _year_month_expr=None):
    """
    년-월별로 제조사-제품군 조합의 개수를 집계하여 반환
    
    Args:
        _lf: LazyFrame
        manufacturer_col: 제조사 컬럼명
        product_col: 제품군(제품코드) 컬럼명
        date_col: 날짜 컬럼명
        selected_dates: 선택된 년-월 리스트
        selected_manufacturers: 선택된 제조사 리스트
        selected_products: 선택된 제품군 리스트
        _year_month_expr: 년-월 컬럼 생성 표현식 (재사용용, 언더스코어로 시작하여 캐싱에서 제외)
    
    Returns:
        년-월별 집계 DataFrame (year_month, manufacturer_product, total_count)
    """
    
    # manufacturer_name과 product_code 조합 생성
    combo_expr = (
        pl.when(pl.col(manufacturer_col).is_not_null() & pl.col(product_col).is_not_null())
        .then(
            pl.col(manufacturer_col).cast(pl.Utf8)
            + pl.lit(" - ")
            + pl.col(product_col).cast(pl.Utf8)
        )
        .otherwise(pl.lit("(정보 없음)"))
        .alias("manufacturer_product")
    )
    
    # 년-월 컬럼 생성 표현식 (재사용 또는 새로 생성)
    year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
    
    # 기본 필터
    filtered_lf = (
        _lf
        .with_columns([combo_expr, year_month_expr])
        .filter(
            pl.col(manufacturer_col).is_not_null() & 
            pl.col(product_col).is_not_null() &
            pl.col(date_col).is_not_null()
        )
    )
    
    # 날짜 필터 적용
    if selected_dates and len(selected_dates) > 0:
        filtered_lf = filtered_lf.filter(pl.col("year_month").is_in(selected_dates))
    
    # 제조사 필터 적용
    if selected_manufacturers and len(selected_manufacturers) > 0:
        filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
    
    # 제품군 필터 적용
    if selected_products and len(selected_products) > 0:
        filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))
    
    # 년-월별, 제조사-제품군별 집계
    result = (
        filtered_lf
        .group_by(["year_month", "manufacturer_product"])
        .agg(pl.len().alias("total_count"))
        .sort(["year_month", "total_count"], descending=[False, True])
        .collect()
    )
    
    return result

@st.cache_data
def get_available_filters(_lf, 
                          manufacturer_col='manufacturer_name',
                          product_col='product_code',
                          date_col='date_received',
                          _year_month_expr=None):
    """
    필터에 사용할 unique 값들을 추출
    
    Args:
        _lf: LazyFrame (언더스코어로 시작하여 캐싱에서 제외)
        manufacturer_col: 제조사 컬럼명
        product_col: 제품군(제품코드) 컬럼명
        date_col: 날짜 컬럼명
        _year_month_expr: 년-월 컬럼 생성 표현식 (재사용용, 언더스코어로 시작하여 캐싱에서 제외)
    
    Returns:
        tuple: (available_dates, available_manufacturers, available_products)
    """
    
    # 년-월 리스트 (재사용 또는 새로 생성)
    year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
    
    try:
        available_dates = (
            _lf
            .filter(pl.col(date_col).is_not_null())
            .with_columns(year_month_expr)
            .select("year_month")
            .filter(pl.col("year_month").is_not_null())
            .unique()
            .sort("year_month", descending=True)
            .collect()
        )["year_month"].to_list()
    except Exception as e:
        available_dates = []
    
    # 제조사 리스트
    available_manufacturers = (
        _lf
        .select(pl.col(manufacturer_col))
        .filter(pl.col(manufacturer_col).is_not_null())
        .unique()
        .sort(manufacturer_col)
        .collect()
    )[manufacturer_col].to_list()
    
    # 제품군 리스트
    available_products = (
        _lf
        .select(pl.col(product_col))
        .filter(pl.col(product_col).is_not_null())
        .unique()
        .sort(product_col)
        .collect()
    )[product_col].to_list()
    
    return available_dates, available_manufacturers, available_products
@st.cache_data
def get_manufacturers_by_dates(_lf, 
                               selected_dates,
                               date_col='date_received',
                               manufacturer_col='manufacturer_name',
                               _year_month_expr=None):
    """
    선택된 년-월에 존재하는 제조사 목록을 반환
    
    Args:
        _lf: LazyFrame
        selected_dates: 선택된 년-월 리스트
        date_col: 날짜 컬럼명
        _year_month_expr: 년-월 컬럼 생성 표현식 (재사용용, 언더스코어로 시작하여 캐싱에서 제외)
    
    Returns:
        선택된 년-월에 존재하는 제조사 목록
    """
    if not selected_dates or len(selected_dates) == 0:
        return []
    
    year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
    
    manufacturers = (
        _lf
        .filter(pl.col(date_col).is_not_null())
        .filter(pl.col(manufacturer_col).is_not_null())
        .with_columns(year_month_expr)
        .filter(pl.col("year_month").is_in(selected_dates))
        .select(pl.col(manufacturer_col))
        .unique()
        .sort(manufacturer_col)
        .collect()
    )[manufacturer_col].to_list()
    
    return manufacturers

@st.cache_data
def get_products_by_manufacturers(_lf, 
                                  selected_manufacturers,
                                  manufacturer_col='manufacturer_name',
                                  product_col='product_code'):
    """
    선택된 제조사에 해당하는 제품군 목록을 반환
    
    Args:
        _lf: LazyFrame (언더스코어로 시작하여 캐싱에서 제외)
        selected_manufacturers: 선택된 제조사 리스트
        manufacturer_col: 제조사 컬럼명
        product_col: 제품군(제품코드) 컬럼명
    
    Returns:
        선택된 제조사에 해당하는 제품군 리스트
    """
    if not selected_manufacturers or len(selected_manufacturers) == 0:
        return []
    
    products = (
        _lf
        .filter(pl.col(manufacturer_col).is_not_null())
        .filter(pl.col(product_col).is_not_null())
        .filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
        .select(pl.col(product_col))
        .unique()
        .sort(product_col)
        .collect()
    )[product_col].to_list()
    
    return products

def get_window_dates(available_dates, window_size, as_of_month=None):
    """
    윈도우 기반 날짜 범위 계산 (최근 k개월과 직전 k개월)
    
    Args:
        available_dates: 사용 가능한 년-월 리스트 (내림차순 정렬된 것 가정)
        window_size: 윈도우 크기 (1 또는 3)
        as_of_month: 기준 월 (None이면 가장 최근 월 사용)
    
    Returns:
        tuple: (recent_months, base_months)
    """
    
    if not available_dates or len(available_dates) == 0:
        return [], []
    
    # 기준 월 설정
    if as_of_month is None:
        as_of_month = available_dates[0]  # 가장 최근 월
    
    # datetime 객체로 변환
    as_of_date = datetime.strptime(as_of_month, "%Y-%m")
    
    # 최근 기간 계산
    if window_size == 1:
        recent_months = [as_of_month]
        base_date = as_of_date - relativedelta(months=1)
        base_months = [base_date.strftime("%Y-%m")]
    else:  # window_size == 3
        recent_months = [
            (as_of_date - relativedelta(months=i)).strftime("%Y-%m")
            for i in range(3)
        ]
        base_months = [
            (as_of_date - relativedelta(months=i)).strftime("%Y-%m")
            for i in range(1, 4)
        ]
    
    # available_dates에 존재하는 월만 필터링
    recent_months = [m for m in recent_months if m in available_dates]
    base_months = [m for m in base_months if m in available_dates]
    
    return recent_months, base_months

def show():
    st.session_state.current_tab = "EDA"
    st.header("Detailed Analysis")
    

    # ==================== 월별 보고서 수 그래프 ====================
    
    # 데이터 로드 및 분석
    lf = load_data()
    
    if lf is not None:
        
        try:
            # 년-월 컬럼 생성 표현식을 한 번만 계산 (재사용)
            date_col = 'date_received'
            year_month_expr = get_year_month_expr(lf, date_col)
            
            # 사용 가능한 필터 옵션 가져오기 (년-월 표현식 재사용)
            with st.spinner("필터 옵션 로딩 중..."):
                available_dates, available_manufacturers, available_products = get_available_filters(
                    lf, 
                    date_col=date_col,
                    _year_month_expr=year_month_expr
                )
            
            # 디버깅 정보
            if len(available_dates) == 0:
                st.warning("사용 가능한 날짜 데이터가 없습니다. 데이터 파일과 날짜 컬럼을 확인해주세요.")
                st.stop()
            
            # 사이드바에서 선택한 년월 및 window 사용
            sidebar_year_month = st.session_state.get('selected_year_month', None)
            sidebar_window = st.session_state.get('selected_window', 1)
            
            # 윈도우 기반 날짜 범위 계산
            if sidebar_year_month:
                recent_months, base_months = get_window_dates(
                    available_dates, 
                    sidebar_window, 
                    sidebar_year_month
                )
                window_info = f"최근 {sidebar_window}개월: {', '.join(recent_months)} | 직전 {sidebar_window}개월: {', '.join(base_months)}"
            else:
                recent_months, base_months = [], []
                window_info = None
            
            # 필터 UI
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # 세션 상태에서 이전 선택값 가져오기
                prev_selected_dates = st.session_state.get('prev_selected_dates', [])
                
                # 사이드바에서 선택한 년월을 기본값으로 사용
                default_dates = [sidebar_year_month] if sidebar_year_month and sidebar_year_month in available_dates else []
                if not default_dates and available_dates:
                    default_dates = [available_dates[0]]
                
                # 이전 선택값이 있고 유효한 경우 우선 사용
                if prev_selected_dates:
                    valid_prev_dates = [d for d in prev_selected_dates if d in available_dates]
                    if valid_prev_dates:
                        default_dates = valid_prev_dates
                
                # 윈도우 기반 자동 선택 옵션
                use_window = st.checkbox(
                    "윈도우 기간 자동 선택 (최근 k개월 + 직전 k개월)",
                    value=st.session_state.get('use_window', True if sidebar_year_month else False),
                    key='use_window_checkbox'
                )
                st.session_state.use_window = use_window
                
                if use_window and sidebar_year_month and recent_months:
                    # 윈도우 기반으로 자동 선택
                    window_dates = list(set(recent_months + base_months))
                    # 이전 선택값이 있으면 유지, 없으면 윈도우 값 사용
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
                
                # 선택값을 세션 상태에 저장
                if selected_dates:
                    st.session_state.prev_selected_dates = selected_dates
                elif 'prev_selected_dates' in st.session_state and not selected_dates:
                    del st.session_state.prev_selected_dates
            
            with col2:
                if selected_dates:
                    #선택된 년-월에 존재하는 제조사만 표시
                    filtered_manufacturers = get_manufacturers_by_dates(
                        lf, 
                        selected_dates,
                        date_col = date_col,
                        _year_month_expr = year_month_expr
                    )

                    # 현재 선택된 제조사 중 필터링된 목록에 없는 것은 제거
                    prev_selected = st.session_state.get('prev_selected_manufacturers', [])
                    valid_selected_manufacturers = [
                        m for m in prev_selected
                        if m in filtered_manufacturers
                    ]
                    
                    manufacturer_options = filtered_manufacturers
                    default_manufacturers = valid_selected_manufacturers
                else:
                    manufacturer_options = available_manufacturers
                    default_manufacturers = []
                    if 'prev_selected_manufacturers' in st.session_state:
                        del st.session_state.prev_selected_manufacturers
                
                if selected_dates:
                    help_text = f"선택된 년-월({len(selected_dates)}개)에 존재하는 제조사만 표시됩니다"
                else:
                    help_text = "제조사를 선택하면 해당 제조사의 제품군만 표시됩니다"

                selected_manufacturers = st.multiselect(
                    "제조사 선택 (선택 안 함 = 전체)",
                    options=manufacturer_options,
                    default=default_manufacturers,
                    help=help_text,
                    key='manufacturers_multiselect'
                )
            
            # 제조사 선택에 따라 제품군 옵션 동적으로 변경
            if selected_manufacturers:
                st.session_state.prev_selected_manufacturers = selected_manufacturers
            else:
                if 'prev_selected_manufacturers' in st.session_state:
                    del st.session_state.prev_selected_manufacturers
            
            with col3:
                if selected_manufacturers:
                    #선택된 제조사에 존재하는 제품군만 표시
                    filtered_products = get_products_by_manufacturers(
                        lf, 
                        selected_manufacturers,
                        manufacturer_col = 'manufacturer_name',
                        product_col = 'product_code'
                    )

                    # 현재 선택된 제품군 중 필터링된 목록에 없는 것은 제거
                    prev_selected = st.session_state.get('prev_selected_products', [])
                    valid_selected_products = [
                        p for p in prev_selected
                        if p in filtered_products
                    ]

                    product_options = filtered_products
                    default_products = valid_selected_products
                else:
                    product_options = available_products
                    default_products = []
                    if 'prev_selected_products' in st.session_state:
                        del st.session_state.prev_selected_products 

                if selected_dates:
                    help_text = f"선택된 년-월({len(selected_dates)}개)에 존재하는 제품군만 표시됩니다"
                else:
                    help_text = "제품군을 선택하면 해당 제품군의 보고 건수만 표시됩니다"

                selected_products = st.multiselect(
                    "제품군 선택 (선택 안 함 = 전체)",
                    options=product_options,
                    default=default_products,
                    help=help_text,
                    key='products_multiselect'
                )
                
                # 현재 선택된 제품군을 저장
                if selected_products:
                    st.session_state.prev_selected_products = selected_products
                elif 'prev_selected_products' in st.session_state and not selected_products:
                    del st.session_state.prev_selected_products
            
            # 상위 N개 선택 (세션 상태에 저장)
            default_top_n = st.session_state.get('top_n', 10)
            top_n = st.number_input(
                "상위 N개 표시", 
                min_value=1, 
                max_value=100, 
                value=default_top_n, 
                step=1,
                key='top_n_input'
            )
            st.session_state.top_n = top_n
            
            # 분석 실행
            with st.spinner("데이터 분석 중..."):
                # 날짜 선택 여부에 따라 다른 데이터 가져오기
                if selected_dates:
                    # 날짜가 선택된 경우: 선택된 기간의 데이터
                    result_df = get_filtered_products(
                        lf,
                        date_col=date_col,
                        selected_dates=selected_dates,
                        selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                        selected_products=selected_products if selected_products else None,
                        top_n=top_n,
                        _year_month_expr=year_month_expr
                    )
                    use_bar_chart = True  # 막대 그래프 사용
                else:
                    # 날짜가 선택되지 않은 경우: 전체 기간의 데이터
                    result_df = get_filtered_products(
                        lf,
                        date_col=date_col,
                        selected_dates=None,  # 전체 기간
                        selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                        selected_products=selected_products if selected_products else None,
                        top_n=top_n,
                        _year_month_expr=year_month_expr
                    )
                    use_bar_chart = False  # 선 그래프 사용
                
                if len(result_df) > 0:
                    # 결과를 pandas DataFrame으로 변환하여 표시
                    display_df = result_df.to_pandas().copy()
                    display_df.insert(0, "순위", range(1, len(display_df) + 1))
                    display_df = display_df[["순위", "manufacturer_product", "total_count"]]
                    display_df.columns = ["순위", "제조사-제품군", "보고 건수"]
                    
                    # 년-월별 집계 데이터 가져오기
                    if selected_dates:
                        # 선택된 기간의 데이터
                        monthly_df = get_monthly_counts(
                            lf,
                            date_col=date_col,
                            selected_dates=selected_dates,
                            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                            selected_products=selected_products if selected_products else None,
                            _year_month_expr=year_month_expr
                        )
                    else:
                        # 전체 기간의 데이터
                        monthly_df = get_monthly_counts(
                            lf,
                            date_col=date_col,
                            selected_dates=None,  # 전체 기간
                            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                            selected_products=selected_products if selected_products else None,
                            _year_month_expr=year_month_expr
                        )
                    
                    if len(monthly_df) > 0:
                        # pandas DataFrame으로 변환
                        monthly_pandas = monthly_df.to_pandas()
                        
                        # 상위 N개 제조사-제품군 조합 선택
                        top_combinations = display_df.head(top_n)["제조사-제품군"].tolist()
                        
                        # 상위 N개 조합만 필터링
                        chart_data = monthly_pandas[
                            monthly_pandas["manufacturer_product"].isin(top_combinations)
                        ].copy()
                        
                        if use_bar_chart:
                            # 막대 그래프: 제조사-제품군별 총 보고 건수
                            bar_chart_data = display_df.head(top_n).set_index("제조사-제품군")[["보고 건수"]]
                            st.bar_chart(bar_chart_data, use_container_width=True)
                        else:
                            # 선 그래프: 년-월별 추이
                            # 피벗 테이블 생성 (년-월을 인덱스로, 제조사-제품군을 컬럼으로)
                            pivot_df = chart_data.pivot_table(
                                index="year_month",
                                columns="manufacturer_product",
                                values="total_count",
                                aggfunc='first',
                                fill_value=0
                            )
                            
                            # 년-월 순서대로 정렬
                            pivot_df = pivot_df.sort_index()
                            
                            # 선 그래프 표시
                            st.line_chart(pivot_df, use_container_width=True)
                    
                    # 표 표시
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                else:
                    st.info("선택한 조건에 해당하는 데이터가 없습니다.")
                
        except Exception as e:
            st.error(f"데이터 분석 중 오류가 발생했습니다: {str(e)}")
            st.exception(e)
    else:
        st.error("데이터 파일을 찾을 수 없습니다. 데이터 경로를 확인해주세요.")

    

    # ==================== 제조사 - 모델별 결함 ====================
    st.subheader("제조사 - 모델별 결함")
    @st.cache_data
    def analyze_manufacturer_defects(_lf,
                                   manufacturer_col='manufacturer_name',
                                   product_col='product_code',
                                   date_col='date_received',
                                   selected_dates=None,
                                   selected_manufacturers=None,
                                   selected_products=None,
                                   _year_month_expr=None):
        """
        제조사-제품군 조합별 결함 분석 (필터 적용)
        """
        # manufacturer_name과 product_code 조합 생성
        combo_expr = (
            pl.when(pl.col(manufacturer_col).is_not_null() & pl.col(product_col).is_not_null())
            .then(
                pl.col(manufacturer_col).cast(pl.Utf8)
                + pl.lit(" - ")
                + pl.col(product_col).cast(pl.Utf8)
            )
            .otherwise(pl.lit("(정보 없음)"))
            .alias("manufacturer_product")
        )

        # 기본 필터링
        filtered_lf = (
            _lf
            .with_columns([combo_expr])
            .filter(
                pl.col(manufacturer_col).is_not_null() & 
                pl.col(product_col).is_not_null()
            )
        )

        # 년-월 필터 적용
        if selected_dates and len(selected_dates) > 0:
            if _year_month_expr is None:
                _year_month_expr = get_year_month_expr(_lf, date_col)
            filtered_lf = (
                filtered_lf
                .with_columns(_year_month_expr)
                .filter(pl.col("year_month").is_in(selected_dates))
            )

        # 제조사 필터 적용
        if selected_manufacturers and len(selected_manufacturers) > 0:
            filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))

        # 제품군 필터 적용
        if selected_products and len(selected_products) > 0:
            filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))

        # 결함 분석 집계
        result = (
            filtered_lf
            .group_by(["manufacturer_product", "defect_type"])
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .with_columns(
                (pl.col("count") / pl.col("count").sum().over("manufacturer_product") * 100)
                .round(2)
                .alias("percentage")
            )
            .sort(["manufacturer_product", "percentage"], descending=[False, True])
            .collect()
        )

        return result
    
    # 결함 분석 실행 및 시각화
    if selected_dates:
        with st.spinner("결함 분석 중..."):
            defect_df = analyze_manufacturer_defects(
                lf,
                manufacturer_col='manufacturer_name',
                product_col='product_code',
                date_col=date_col,
                selected_dates=selected_dates,
                selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                selected_products=selected_products if selected_products else None,
                _year_month_expr=year_month_expr
            )
        
        if len(defect_df) > 0:
            # 결과를 pandas DataFrame으로 변환
            display_df = defect_df.to_pandas()
            
            # 제조사-제품군별로 그룹화하여 막대 차트 생성
            # 각 제조사-제품군별로 결함 유형별 비율을 막대 차트로 표시
            
            # 제조사-제품군 목록 가져오기
            unique_manufacturers = display_df["manufacturer_product"].unique()
            
            if len(unique_manufacturers) > 0:
                # 비교 모드 선택
                view_mode = st.radio(
                    "보기 모드",
                    options=["단일 제조사-제품군", "전체 비교"],
                    horizontal=True
                )
                
                if view_mode == "단일 제조사-제품군":
                    # 제조사-제품군 선택 드롭다운
                    selected_manufacturer = st.selectbox(
                        "제조사-제품군 선택",
                        options=unique_manufacturers,
                        index=0
                    )
                    
                    # 선택된 제조사-제품군의 데이터 필터링
                    mfr_data = display_df[
                        display_df["manufacturer_product"] == selected_manufacturer
                    ].copy()
                    
                    if len(mfr_data) > 0:
                        # 막대 차트 데이터 준비
                        chart_data = pd.DataFrame({
                            "결함 유형": mfr_data["defect_type"],
                            "건수": mfr_data["count"],
                            "비율(%)": mfr_data["percentage"]
                        }).sort_values("건수", ascending=False)
                        
                        # 막대 차트 표시 (비율 기준)
                        st.bar_chart(
                            chart_data.set_index("결함 유형")[["비율(%)"]],
                            use_container_width=True
                        )
                        
                        # 상세 데이터 테이블
                        st.dataframe(
                            chart_data[["결함 유형", "건수", "비율(%)"]],
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info(f"{selected_manufacturer}에 대한 결함 데이터가 없습니다.")
                
                else:  # 전체 비교 모드
                    # 모든 제조사-제품군의 결함 유형별 비율을 비교
                    # 피벗 테이블 생성: 제조사-제품군을 컬럼으로, 결함 유형을 인덱스로
                    pivot_df = display_df.pivot_table(
                        index="defect_type",
                        columns="manufacturer_product",
                        values="percentage",
                        aggfunc='first',
                        fill_value=0
                    )
                    
                    # 막대 차트로 전체 비교 표시
                    st.bar_chart(
                        pivot_df,
                        use_container_width=True
                    )
                    
                    # 전체 데이터 테이블
                    st.dataframe(
                        display_df[["manufacturer_product", "defect_type", "count", "percentage"]]
                        .sort_values(["manufacturer_product", "count"], ascending=[True, False])
                        .rename(columns={
                            "manufacturer_product": "제조사-제품군",
                            "defect_type": "결함 유형",
                            "count": "건수",
                            "percentage": "비율(%)"
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
            else:
                st.info("결함 데이터가 없습니다.")
        else:
            st.info("선택한 조건에 해당하는 결함 데이터가 없습니다.")
    else:
        st.info("결함 분석을 위해 년-월을 선택해주세요.")

    # ==================== 문제 부품 분석 ====================
    st.subheader("문제 부품 분석")
    
    @st.cache_data
    def get_available_defect_types(_lf,
                                   manufacturer_col='manufacturer_name',
                                   product_col='product_code',
                                   date_col='date_received',
                                   selected_dates=None,
                                   selected_manufacturers=None,
                                   selected_products=None,
                                   _year_month_expr=None):
        """
        필터링된 데이터에서 사용 가능한 결함 유형 목록 반환
        """
        year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
        
        filtered_lf = _lf.filter(pl.col('defect_type').is_not_null())
        
        # 날짜 필터 적용
        if selected_dates and len(selected_dates) > 0:
            filtered_lf = (
                filtered_lf
                .with_columns(year_month_expr)
                .filter(pl.col("year_month").is_in(selected_dates))
            )
        
        # 제조사 필터 적용
        if selected_manufacturers and len(selected_manufacturers) > 0:
            filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
        
        # 제품군 필터 적용
        if selected_products and len(selected_products) > 0:
            filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))
        
        defect_types = (
            filtered_lf
            .select(pl.col('defect_type'))
            .unique()
            .sort('defect_type')
            .collect()
        )['defect_type'].to_list()
        
        return defect_types
    
    @st.cache_data
    def analyze_defect_components(_lf,
                                  defect_type,
                                  manufacturer_col='manufacturer_name',
                                  product_col='product_code',
                                  date_col='date_received',
                                  selected_dates=None,
                                  selected_manufacturers=None,
                                  selected_products=None,
                                  top_n=10,
                                  _year_month_expr=None):
        """
        특정 결함 종류의 문제 기기 부품 분석
        
        Args:
            _lf: LazyFrame
            defect_type: 분석할 결함 종류
            manufacturer_col: 제조사 컬럼명
            product_col: 제품군 컬럼명
            date_col: 날짜 컬럼명
            selected_dates: 선택된 년-월 리스트
            selected_manufacturers: 선택된 제조사 리스트
            selected_products: 선택된 제품군 리스트
            top_n: 상위 N개 문제 부품 표시
            _year_month_expr: 년-월 컬럼 생성 표현식
        
        Returns:
            문제 부품 분포 DataFrame
        """
        year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
        
        # 기본 필터링
        filtered_lf = _lf.filter(pl.col('defect_type') == defect_type)
        
        # 날짜 필터 적용
        if selected_dates and len(selected_dates) > 0:
            filtered_lf = (
                filtered_lf
                .with_columns(year_month_expr)
                .filter(pl.col("year_month").is_in(selected_dates))
            )
        
        # 제조사 필터 적용
        if selected_manufacturers and len(selected_manufacturers) > 0:
            filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
        
        # 제품군 필터 적용
        if selected_products and len(selected_products) > 0:
            filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))
        
        # problem_components가 null이 아닌 데이터만 필터링
        defect_data = filtered_lf.filter(pl.col('problem_components').is_not_null())
        
        # 전체 개수 계산
        total = defect_data.select(pl.len()).collect().item()
        
        if total == 0:
            return None
        
        # 문제 부품 분포 집계
        component_dist = (
            defect_data
            .group_by('problem_components')
            .agg(pl.len().alias('count'))
            .with_columns(
                (pl.col('count') / total * 100)
                .round(2)
                .alias('percentage')
            )
            .sort('count', descending=True)
            .head(top_n)
            .collect()
        )
        
        return component_dist
    
    # 문제 부품 분석 UI
    if lf is not None and selected_dates:
        try:
            # 사용 가능한 결함 유형 가져오기
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
                # 결함 유형 선택
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # 이전에 선택한 결함 유형 가져오기
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
                    # 선택값 저장
                    st.session_state.prev_selected_defect_type = selected_defect_type
                
                with col2:
                    default_top_n_components = st.session_state.get('top_n_components', 10)
                    top_n_components = st.number_input(
                        "상위 N개 표시",
                        min_value=1,
                        max_value=50,
                        value=default_top_n_components,
                        step=1,
                        key='top_n_components_input'
                    )
                    st.session_state.top_n_components = top_n_components
                
                # 문제 부품 분석 실행
                if selected_defect_type:
                    with st.spinner("문제 부품 분석 중..."):
                        component_df = analyze_defect_components(
                            lf,
                            defect_type=selected_defect_type,
                            manufacturer_col='manufacturer_name',
                            product_col='product_code',
                            date_col=date_col,
                            selected_dates=selected_dates,
                            selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                            selected_products=selected_products if selected_products else None,
                            top_n=top_n_components,
                            _year_month_expr=year_month_expr
                        )
                    
                    if component_df is not None and len(component_df) > 0:
                        # 결과를 pandas DataFrame으로 변환
                        display_df = component_df.to_pandas().copy()
                        
                        # problem_components를 문자열로 변환 (리스트 타입인 경우)
                        display_df['problem_components'] = display_df['problem_components'].apply(
                            lambda x: str(x) if x is not None else "(NULL)"
                        )
                        
                        # 표시용 컬럼명 변경
                        display_df.insert(0, "순위", range(1, len(display_df) + 1))
                        display_df = display_df[["순위", "problem_components", "count", "percentage"]]
                        display_df.columns = ["순위", "문제 부품", "건수", "비율(%)"]
                        
                        # # 막대 차트 표시 (건수 기준)
                        # chart_data = display_df.set_index("문제 부품")[["건수"]]
                        # st.bar_chart(chart_data, use_container_width=True)
                        
                        # 표 표시
                        st.dataframe(
                            display_df,
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.info(f"'{selected_defect_type}' 결함 유형에 대한 문제 부품 데이터가 없습니다.")
            else:
                st.info("선택한 조건에 해당하는 결함 유형이 없습니다.")
                
        except Exception as e:
            st.error(f"문제 부품 분석 중 오류가 발생했습니다: {str(e)}")
            st.exception(e)
    elif lf is not None:
        st.info("문제 부품 분석을 위해 년-월을 선택해주세요.")

    # ==================== 기기별 치명률(CFR) 분석 ====================
    st.subheader("기기별 치명률(CFR) 분석")
    
    @st.cache_data
    def calculate_cfr_by_device(_lf,
                                manufacturer_col='manufacturer_name',
                                product_col='product_code',
                                event_column='event_type',
                                date_col='date_received',
                                selected_dates=None,
                                selected_manufacturers=None,
                                selected_products=None,
                                top_n=None,
                                min_cases=10,
                                _year_month_expr=None):
        """
        제조사-제품군 조합별 치명률(Case Fatality Rate)을 계산하는 함수
        
        치명률(CFR) = (사망 건수 / 해당 기기 총 보고 건수) × 100
        
        Args:
            _lf: LazyFrame
            manufacturer_col: 제조사 컬럼명
            product_col: 제품군 컬럼명
            event_column: 사건 유형 컬럼명
            date_col: 날짜 컬럼명
            selected_dates: 선택된 년-월 리스트
            selected_manufacturers: 선택된 제조사 리스트
            selected_products: 선택된 제품군 리스트
            top_n: 상위 N개 기기만 분석 (None이면 전체)
            min_cases: 최소 보고 건수 (이보다 적은 기기는 제외, 통계적 신뢰도 확보)
            _year_month_expr: 년-월 컬럼 생성 표현식
        
        Returns:
            기기별 치명률 결과 DataFrame
        """
        year_month_expr = _year_month_expr if _year_month_expr is not None else get_year_month_expr(_lf, date_col)
        
        # manufacturer_name과 product_code 조합 생성
        combo_expr = (
            pl.when(pl.col(manufacturer_col).is_not_null() & pl.col(product_col).is_not_null())
            .then(
                pl.col(manufacturer_col).cast(pl.Utf8)
                + pl.lit(" - ")
                + pl.col(product_col).cast(pl.Utf8)
            )
            .otherwise(pl.lit("(정보 없음)"))
            .alias("manufacturer_product")
        )
        
        # 기본 필터링
        filtered_lf = (
            _lf
            .with_columns([combo_expr])
            .filter(
                pl.col(manufacturer_col).is_not_null() & 
                pl.col(product_col).is_not_null()
            )
        )
        
        # 날짜 필터 적용
        if selected_dates and len(selected_dates) > 0:
            filtered_lf = (
                filtered_lf
                .with_columns(year_month_expr)
                .filter(pl.col("year_month").is_in(selected_dates))
            )
        
        # 제조사 필터 적용
        if selected_manufacturers and len(selected_manufacturers) > 0:
            filtered_lf = filtered_lf.filter(pl.col(manufacturer_col).is_in(selected_manufacturers))
        
        # 제품군 필터 적용
        if selected_products and len(selected_products) > 0:
            filtered_lf = filtered_lf.filter(pl.col(product_col).is_in(selected_products))
        
        # 제조사-제품군 조합별 전체 건수와 사건 유형별 건수
        device_stats = (
            filtered_lf
            .group_by("manufacturer_product")
            .agg([
                pl.len().alias('total_cases'),
                (pl.col(event_column) == 'Death').sum().alias('death_count'),
                (pl.col(event_column) == 'Injury').sum().alias('injury_count'),
                (pl.col(event_column) == 'Malfunction').sum().alias('malfunction_count')
            ])
            .filter(pl.col('total_cases') >= min_cases)  # 최소 건수 필터
            .with_columns([
                # CFR 계산
                (pl.col('death_count') / pl.col('total_cases') * 100).round(2).alias('cfr'),
                # 부상률
                (pl.col('injury_count') / pl.col('total_cases') * 100).round(2).alias('injury_rate'),
                # 오작동률
                (pl.col('malfunction_count') / pl.col('total_cases') * 100).round(2).alias('malfunction_rate')
            ])
            .sort('cfr', descending=True)
        )
        
        # Top N만
        if top_n:
            device_stats = device_stats.head(top_n)
        
        result = device_stats.collect()
        
        return result
    
    # 기기별 치명률 분석 UI
    if lf is not None:
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
                default_min_cases = st.session_state.get('min_cases', 10)
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
            
            # 분석 실행
            with st.spinner("기기별 치명률 분석 중..."):
                cfr_result = calculate_cfr_by_device(
                    lf,
                    manufacturer_col='manufacturer_name',
                    product_col='product_code',
                    event_column='event_type',
                    date_col=date_col,
                    selected_dates=selected_dates if selected_dates else None,
                    selected_manufacturers=selected_manufacturers if selected_manufacturers else None,
                    selected_products=selected_products if selected_products else None,
                    top_n=top_n_cfr if top_n_cfr else None,
                    min_cases=min_cases,
                    _year_month_expr=year_month_expr
                )
            
            if len(cfr_result) > 0:
                # 결과를 pandas DataFrame으로 변환
                display_df = cfr_result.to_pandas().copy()
                
                # 표시용 컬럼명 변경
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
                
                # 표 표시
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )
                
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
