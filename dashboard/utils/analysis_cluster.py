# analysis_cluster.py
"""Cluster/Defect Type 분석 관련 함수"""

import polars as pl
import streamlit as st
import ast
from typing import List, Optional
from .constants import ColumnNames, Defaults
from .data_utils import apply_basic_filters


@st.cache_data
def get_available_clusters(
    _lf: pl.LazyFrame,
    cluster_col: str = ColumnNames.DEFECT_TYPE,
    date_col: str = ColumnNames.DATE_RECEIVED,
    selected_dates: Optional[List[str]] = None,
    selected_manufacturers: Optional[List[str]] = None,
    selected_products: Optional[List[str]] = None,
    _year_month_expr: Optional[pl.Expr] = None
) -> List[str]:
    """필터링된 데이터에서 사용 가능한 defect type 목록 반환

    Args:
        _lf: LazyFrame
        cluster_col: defect type 컬럼명
        date_col: 날짜 컬럼명
        selected_dates: 선택된 년-월 리스트
        selected_manufacturers: 선택된 제조사 리스트
        selected_products: 선택된 제품군 리스트
        _year_month_expr: 년-월 컬럼 생성 표현식

    Returns:
        defect type 리스트
    """
    # 기본 필터 적용
    filtered_lf = apply_basic_filters(
        _lf,
        manufacturer_col=ColumnNames.MANUFACTURER,
        product_col=ColumnNames.PRODUCT_CODE,
        date_col=date_col,
        selected_dates=selected_dates,
        selected_manufacturers=selected_manufacturers,
        selected_products=selected_products,
        year_month_expr=_year_month_expr,
        add_combo=False
    )

    filtered_lf = filtered_lf.filter(pl.col(cluster_col).is_not_null())

    clusters = (
        filtered_lf
        .select(pl.col(cluster_col))
        .unique()
        .sort(cluster_col)
        .collect()
    )[cluster_col].to_list()

    return clusters


@st.cache_data
def cluster_keyword_unpack(
    _lf: pl.LazyFrame,
    col_name: str = ColumnNames.PROBLEM_COMPONENTS,
    cluster_col: str = ColumnNames.DEFECT_TYPE,
    date_col: str = ColumnNames.DATE_RECEIVED,
    selected_dates: Optional[List[str]] = None,
    selected_manufacturers: Optional[List[str]] = None,
    selected_products: Optional[List[str]] = None,
    top_n: int = Defaults.TOP_N,
    _year_month_expr: Optional[pl.Expr] = None
) -> pl.DataFrame:
    """defect type 별로 col_name마다 있는 리스트를 열어서 키워드 종류를 추출하고 count

    Args:
        _lf: LazyFrame
        col_name: 리스트가 들어있는 열 이름 (예: 'problem_components')
        cluster_col: defect type 열 이름
        date_col: 날짜 컬럼명
        selected_dates: 선택된 년-월 리스트
        selected_manufacturers: 선택된 제조사 리스트
        selected_products: 선택된 제품군 리스트
        top_n: 상위 N개 키워드만 반환
        _year_month_expr: 년-월 컬럼 생성 표현식

    Returns:
        defect type별 키워드, count, ratio를 포함한 DataFrame
    """
    # 기본 필터 적용
    lf_temp = apply_basic_filters(
        _lf,
        manufacturer_col=ColumnNames.MANUFACTURER,
        product_col=ColumnNames.PRODUCT_CODE,
        date_col=date_col,
        selected_dates=selected_dates,
        selected_manufacturers=selected_manufacturers,
        selected_products=selected_products,
        year_month_expr=_year_month_expr,
        add_combo=False
    )

    # 필요한 컬럼만 선택
    lf_temp = lf_temp.select([cluster_col, col_name])

    # 1. 문자열을 리스트로 변환 (필요한 경우)
    schema = lf_temp.collect_schema()
    if schema[col_name] == pl.Utf8:
        def safe_literal_eval(x):
            if not x or x == 'null' or x == 'None':
                return []
            try:
                result = ast.literal_eval(x)
                return result if isinstance(result, list) else []
            except (ValueError, SyntaxError):
                return []

        lf_temp = lf_temp.with_columns(
            pl.col(col_name)
            .map_elements(safe_literal_eval, return_dtype=pl.List(pl.Utf8))
        )

    # 2. 전체 데이터를 한 번에 explode (벡터화)
    exploded_lf = (lf_temp
                  .explode(col_name)
                  .filter(pl.col(col_name).is_not_null())
                  .filter(pl.col(col_name) != "")  # 빈 문자열 제거
                 )

    # 3. defect type별로 그룹화하여 카운트 (벡터화)
    keyword_counts = (exploded_lf
                      .with_columns(
                          pl.col(col_name).str.to_lowercase().str.strip_chars()  # 소문자 + 공백 제거
                          )
                      .group_by([cluster_col, col_name])
                      .agg(pl.len().alias('count'))
                     )

    # 4. defect type별 전체 키워드 수 계산
    cluster_totals = (keyword_counts
                      .group_by(cluster_col)
                      .agg(pl.col('count').sum().alias('total_count'))
                     )

    # 5. ratio 계산 및 정렬
    result_lf = (keyword_counts
                 .join(cluster_totals, on=cluster_col)
                 .with_columns(
                     (pl.col('count') / pl.col('total_count') * 100).round(2).alias('ratio')
                 )
                 .select([cluster_col, col_name, 'count', 'ratio'])
                 .sort([cluster_col, 'count'], descending=[False, True])
                )

    # 6. defect type별 상위 N개만 선택
    result_df = (
        result_lf
        .with_columns(
            pl.col('count').rank('dense', descending=True).over(cluster_col).alias('rank')
        )
        .filter(pl.col('rank') <= top_n)
        .drop('rank')
        .collect()
    )

    return result_df


@st.cache_data
def get_event_type_summary(
    _lf: pl.LazyFrame,
    event_column: str = ColumnNames.EVENT_TYPE,
    date_col: str = ColumnNames.DATE_RECEIVED,
    selected_dates: Optional[List[str]] = None,
    selected_manufacturers: Optional[List[str]] = None,
    selected_products: Optional[List[str]] = None,
    _year_month_expr: Optional[pl.Expr] = None
) -> dict:
    """사건 유형별 분포 계산 (파이 차트용)

    Args:
        _lf: LazyFrame
        event_column: 사건 유형 컬럼명
        date_col: 날짜 컬럼명
        selected_dates: 선택된 년-월 리스트
        selected_manufacturers: 선택된 제조사 리스트
        selected_products: 선택된 제품군 리스트
        _year_month_expr: 년-월 컬럼 생성 표현식

    Returns:
        {
            'total_deaths': 사망 건수,
            'total_injuries': 부상 건수,
            'total_malfunctions': 오작동 건수,
            'total_all': 전체 건수
        }
    """
    # 기본 필터 적용
    filtered_lf = apply_basic_filters(
        _lf,
        manufacturer_col=ColumnNames.MANUFACTURER,
        product_col=ColumnNames.PRODUCT_CODE,
        date_col=date_col,
        selected_dates=selected_dates,
        selected_manufacturers=selected_manufacturers,
        selected_products=selected_products,
        year_month_expr=_year_month_expr,
        add_combo=False
    )

    # 사건 유형별 집계
    result = filtered_lf.select([
        (pl.col(event_column) == 'Death').sum().alias('death_count'),
        (pl.col(event_column) == 'Injury').sum().alias('injury_count'),
        (pl.col(event_column) == 'Malfunction').sum().alias('malfunction_count')
    ]).collect()

    total_deaths = result['death_count'][0] if len(result) > 0 else 0
    total_injuries = result['injury_count'][0] if len(result) > 0 else 0
    total_malfunctions = result['malfunction_count'][0] if len(result) > 0 else 0

    return {
        'total_deaths': total_deaths,
        'total_injuries': total_injuries,
        'total_malfunctions': total_malfunctions,
        'total_all': total_deaths + total_injuries + total_malfunctions
    }
