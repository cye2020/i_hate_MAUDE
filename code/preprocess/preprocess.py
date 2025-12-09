import re
from typing import List, Dict, Tuple, Union
import polars as pl
import plotly.graph_objects as go
from IPython import display


def get_pattern_cols(
    lf: pl.LazyFrame,
    pattern: List[str],
) -> List[str]:
    """정규표현식 패턴에 매칭되는 컬럼명 추출
    
    Args:
        lf (pl.LazyFrame): 대상 LazyFrame
        pattern (List[str]): 정규표현식 패턴 리스트
    
    Returns:
        List[str]: 패턴에 매칭되는 컬럼명 리스트
    
    Examples:
        >>> get_pattern_cols(lf, [r'^device_\d+', r'.*_date$'])
        ['device_0_name', 'device_1_name', 'report_date', 'event_date']
    """
    # 모든 컬럼명 가져오기
    cols = lf.collect_schema().names()
    
    # 패턴 문자열을 정규표현식 객체로 컴파일
    regexes = [re.compile(p) for p in pattern]
    
    # 각 컬럼명이 패턴 중 하나라도 매칭되면 포함
    return [c for c in cols if any(r.search(c) for r in regexes)]


def get_use_cols(
    lf: pl.LazyFrame,
    patterns: Dict[str, List[str]],
    base_cols: List[str],
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """기본 컬럼과 패턴별 컬럼을 합쳐 분석용 컬럼 리스트 생성
    
    Args:
        lf (pl.LazyFrame): 대상 LazyFrame
        patterns (Dict[str, List[str]]): 카테고리별 정규표현식 패턴 딕셔너리
            예: {'device': [r'^device_'], 'patient': [r'^patient_']}
        base_cols (List[str]): 기본적으로 포함할 컬럼 리스트
    
    Returns:
        Tuple[List[str], Dict[str, List[str]]]: 
            - 전체 분석 컬럼 리스트 (중복 제거, 역순 정렬)
            - 카테고리별 컬럼 딕셔너리
    
    Examples:
        >>> patterns = {'device': [r'^device_'], 'event': [r'event_']}
        >>> base_cols = ['report_id', 'date_received']
        >>> all_cols, pattern_cols = get_use_cols(lf, patterns, base_cols)
    """
    # 기본 컬럼으로 시작
    analysis_cols = base_cols
    
    # 패턴별로 컬럼 추출 및 저장
    pattern_cols = {}
    for k, pattern in patterns.items():
        pattern_cols[k] = get_pattern_cols(lf, pattern)
        analysis_cols += pattern_cols[k]
    
    # 중복 제거 후 역순 정렬
    analysis_cols = sorted(list(set(analysis_cols)), reverse=True)
    
    # 요약 정보 출력
    print(f"총 컬럼: {len(analysis_cols)}개")
    for k, pattern in pattern_cols.items():
        print(f"{k} 컬럼: {len(pattern)}개")
    
    return analysis_cols, pattern_cols


def draw_donut_chart(count_df: pl.DataFrame, col: str, top_n: int = 5) -> None:
    """데이터프레임을 기반으로 도넛 차트 생성
    
    상위 N개 항목을 표시하고 나머지는 'Minor Group'으로 묶어서 표시
    
    Args:
        count_df (pl.DataFrame): 'count'와 'percentage' 컬럼을 포함한 DataFrame
        col (str): 레이블로 사용할 컬럼명
        top_n (int, optional): 개별 표시할 상위 항목 개수. Defaults to 5.
    
    Returns:
        None: Plotly 차트를 화면에 표시
    
    Examples:
        >>> count_df = pl.DataFrame({
        ...     'category': ['A', 'B', 'C', 'D', 'E', 'F'],
        ...     'count': [100, 80, 60, 40, 20, 10]
        ... })
        >>> draw_donut_chart(count_df, 'category', top_n=3)
    """
    # top_n이 지정되고 데이터가 그보다 많으면 처리
    if top_n and len(count_df) > top_n:
        # 상위 N개 추출
        top = count_df.head(top_n)
        
        # 나머지 합계 계산
        rest_sum = count_df[top_n:].select(pl.col('count').sum()).item()
        
        if rest_sum > 0:
            # 나머지 비율 계산
            rest_percentage = round(rest_sum / count_df.select(pl.col('count').sum()).item() * 100, 2)
            
            # 'Minor Group' 행 생성
            other_row = pl.DataFrame({
                col: ['Minor Group'],
                'count': [rest_sum],
                'percentage': [rest_percentage]
            }).with_columns(
                pl.col('count').cast(count_df['count'].dtype)  # 타입 맞추기
            )
            
            # 상위 N개와 Minor Group 합치기
            count_df = pl.concat([top, other_row])
        else:
            count_df = top
    
    # Plotly 도넛 차트 생성
    fig = go.Figure(data=[go.Pie(
        labels=count_df[col],
        values=count_df['count'],
        hole=.4,  # 가운데 구멍 크기 (도넛 모양)
        hoverinfo="label+percent",  # 호버 시 표시 정보
        textinfo='label+percent',  # 차트에 표시할 정보
        textposition='outside'  # 텍스트 위치
    )])
    
    # 컬럼명을 제목으로 변환 (중간에 줄바꿈 추가)
    title = col.title().split('_')
    text = ' '.join(title[:len(title)//2]) + '<br>' + ' '.join(title[len(title)//2:])
    
    # 레이아웃 설정
    fig.update_layout(
        title_text=f"{col.title()} Distribution",
        annotations=[dict(
            text=text,  # 도넛 중앙에 표시할 텍스트
            x=0.5, 
            y=0.5,
            font_size=20, 
            showarrow=False
        )]
    )
    
    fig.show()
    

def eda_proportion(lf: pl.LazyFrame, col: str, n_rows: int = 100, top_n: int = 5) -> None:
    """특정 컬럼의 값 분포를 테이블과 도넛 차트로 시각화
    
    값의 빈도와 비율을 계산하여 테이블로 표시하고, 도넛 차트로 시각화
    
    Args:
        lf (pl.LazyFrame): 분석할 LazyFrame
        col (str): 분석할 컬럼명
        n_rows (int, optional): 테이블에 표시할 최대 행 수. Defaults to 100.
        top_n (int, optional): 차트에 개별 표시할 상위 항목 개수. Defaults to 5.
    
    Returns:
        None: 테이블과 차트를 화면에 표시
    
    Examples:
        >>> eda_proportion(lf, 'device_class', n_rows=50, top_n=3)
    """
    # 값별 카운트 및 비율 계산
    count_lf = lf.select(
        pl.col(col).value_counts(sort=True)  # 빈도수 계산 및 정렬
    ).unnest(col).with_columns(
        (pl.col('count') / pl.col('count').sum() * 100).round(2).alias('percentage')  # 백분율 계산
    ).sort(by='count', descending=True).head(n_rows)
    
    # 테이블 표시
    display.display(count_lf.collect().to_pandas())
    
    # 도넛 차트 표시
    draw_donut_chart(count_lf.collect(), col, top_n)


def overview_col(lf: pl.LazyFrame, col: str, n_rows: int = 100) -> None:
    """특정 컬럼의 고유값 개수와 샘플 값들을 표시
    
    컬럼의 고유값(unique) 개수를 출력하고, 상위/하위 샘플 값들을 테이블로 표시
    
    Args:
        lf (pl.LazyFrame): 분석할 LazyFrame
        col (str): 분석할 컬럼명
        n_rows (int, optional): 표시할 샘플 개수. Defaults to 100.
    
    Returns:
        None: 고유값 개수와 샘플 테이블을 화면에 표시
    
    Examples:
        >>> overview_col(lf, 'manufacturer_name', n_rows=50)
        manufacturer_name의 고유 개수: 1234
        [head/tail 샘플 테이블 표시]
    """
    # 고유값 개수 계산
    nunique = lf.select(
        pl.col(col).n_unique().alias(f'unique_{col}')
    ).collect().item()
    
    print(f'{col}의 고유 개수: {nunique}')
    
    # 고유값을 정렬하여 상위/하위 샘플 추출
    unique_lf = lf.select(
        pl.col(col).unique().sort().head(n_rows).alias(f'head_{col}'),  # 상위 n개
        pl.col(col).unique().sort().tail(n_rows).alias(f'tail_{col}'),  # 하위 n개
    )
    
    # 테이블 표시
    display.display(unique_lf.collect().to_pandas())
    

def analyze_null_values(lf: pl.LazyFrame, analysis_cols=None, verbose=True) -> pl.DataFrame:
    """전체 컬럼의 결측치(null) 개수와 비율을 분석
    
    각 컬럼별 결측치 개수와 비율을 계산하여 내림차순으로 정렬된 DataFrame 반환
    
    Args:
        lf (pl.LazyFrame): 분석할 LazyFrame
        analysis_cols (List[str], optional): 분석할 컬럼 리스트. None이면 전체 컬럼. Defaults to None.
        verbose (bool, optional): 결과를 출력할지 여부. Defaults to True.
    
    Returns:
        pl.DataFrame: 'column', 'null_count', 'null_pct' 컬럼을 포함한 결측치 분석 결과
            - column: 컬럼명
            - null_count: 결측치 개수
            - null_pct: 결측치 비율(%)
    
    Examples:
        >>> null_df = analyze_null_values(lf, verbose=True)
        === 결측치 분석 ===
        전체 행 수: 1,000,000
        
        patient_age                                  :    500,000개 ( 50.00%)
        device_model                                 :    300,000개 ( 30.00%)
        ...
    """
    # 분석할 컬럼 결정 (None이면 전체 컬럼)
    if analysis_cols is None:
        analysis_cols = lf.collect_schema().names()
    
    # 전체 행 수 계산
    total_rows = lf.select(pl.len()).collect().item()
    
    # 각 컬럼의 null count를 한 번에 계산
    null_df = (
        lf.select([pl.col(col).null_count().alias(col) for col in analysis_cols])
        .collect()
        .transpose(include_header=True, header_name='column', column_names=['null_count'])  # 전치
        .with_columns(
            (pl.col('null_count') / total_rows * 100).round(2).alias('null_pct')  # 백분율 계산
        )
        .sort('null_pct', descending=True)  # 결측치 비율 내림차순 정렬
    )
    
    # verbose 모드일 경우 결과 출력
    if verbose:
        print("\n=== 결측치 분석 ===")
        print(f"전체 행 수: {total_rows:,}\n")
        for row in null_df.iter_rows(named=True):
            print(f"{row['column']:45s}: {row['null_count']:>10,}개 ({row['null_pct']:>6.2f}%)")
    
    return null_df


def safe_groupby_unique(
    lf: pl.LazyFrame, 
    group_cols: List[str],
    agg_cols: List[str] = None,
    top_n: int = 100,
    streaming: bool = True
) -> pl.DataFrame:
    """메모리 효율적으로 group by 후 각 그룹의 행 개수와 unique 개수 계산
    
    대용량 데이터에서 메모리 오버플로우 없이 그룹별 집계를 수행
    streaming 옵션으로 메모리 사용량 최소화
    
    Parameters:
    -----------
    lf : pl.LazyFrame
        분석할 LazyFrame
    group_cols : List[str]
        그룹화할 컬럼 리스트
    agg_cols : List[str], optional
        unique 개수를 셀 컬럼 리스트. None이면 count만 계산. Defaults to None.
    top_n : int, optional
        상위 몇 개 그룹만 반환할지. Defaults to 100.
    streaming : bool, optional
        streaming 엔진 사용 여부 (메모리 효율성 향상). Defaults to True.
    
    Returns:
    --------
    pl.DataFrame: 그룹별 집계 결과 DataFrame
        - group_cols: 그룹화 컬럼들
        - count: 각 그룹의 행 개수
        - {col}_unique: 각 컬럼의 고유값 개수 (agg_cols 지정 시)
    
    Examples:
    ---------
    >>> # 단순 카운트만
    >>> result = safe_groupby_unique(
    ...     lf, 
    ...     group_cols=['device_model', 'brand_name'],
    ...     top_n=50
    ... )
    
    >>> # unique 개수도 함께 계산
    >>> result = safe_groupby_unique(
    ...     lf,
    ...     group_cols=['device_model', 'brand_name'],
    ...     agg_cols=['report_id', 'event_type'],
    ...     top_n=100,
    ...     streaming=True
    ... )
    """
    # 집계 표현식 구성
    if agg_cols is None:
        # count만 계산
        agg_exprs = [pl.len().alias('count')]
    else:
        # count + 각 컬럼의 unique 개수 계산
        agg_exprs = [
            pl.len().alias('count')
        ] + [
            pl.col(col).n_unique().alias(f'{col}_unique')
            for col in agg_cols
        ]
    
    # streaming 여부에 따라 엔진 선택
    engine = 'streaming' if streaming else 'auto'
    
    # group by 후 집계, 정렬, 상위 N개만 반환
    return (
        lf.group_by(group_cols)
        .agg(agg_exprs)
        .sort('count', descending=True)  # count 기준 내림차순
        .head(top_n)  # 상위 N개만
        .collect(engine=engine)  # 지정된 엔진으로 실행
        .to_pandas()  # pandas DataFrame으로 변환
    )
    

def replace_pattern_with_null(lf: pl.LazyFrame, cols: Union[str, List[str]], na_pattern: str) -> pl.LazyFrame:
    """지정된 컬럼들에서 정규식 패턴과 매칭되는 값을 null로 변경
    
    대소문자 구분 없이 패턴과 매칭되는 모든 값을 null로 치환합니다.
    결측치를 나타내는 다양한 표현('N/A', 'UNKNOWN', 'NONE' 등)을 통일된 null로 변환할 때 유용합니다.
    
    Parameters:
    -----------
    lf : pl.LazyFrame
        처리할 LazyFrame
    cols : str or List[str]
        처리할 컬럼명 (단일 컬럼 문자열 또는 컬럼명 리스트)
    na_pattern : str
        null로 변경할 정규식 패턴
        예: r'^(N/A|UNKNOWN|NONE|NA)$' - 정확히 이 값들만 매칭
            r'UNKNOWN' - UNKNOWN이 포함된 모든 값 매칭
    
    Returns:
    --------
    pl.LazyFrame
        패턴에 매칭된 값이 null로 변경된 LazyFrame
    
    Examples:
    ---------
    >>> # 단일 컬럼 처리
    >>> lf = replace_pattern_with_null(lf, 'device_name', r'^(N/A|UNKNOWN)$')
    
    >>> # 여러 컬럼 동시 처리
    >>> lf = replace_pattern_with_null(
    ...     lf, 
    ...     ['device_name', 'manufacturer', 'model'], 
    ...     r'^(N/A|UNKNOWN|NONE|NA|-|NULL)$'
    ... )
    
    Notes:
    ------
    - 대소문자를 구분하지 않습니다 (자동으로 대문자로 변환 후 비교)
    - 원본 컬럼명을 유지합니다 (.name.keep())
    """
    # 단일 컬럼 문자열을 리스트로 변환
    if isinstance(cols, str):
        cols = [cols]
    
    # 패턴 매칭된 값을 null로 변경
    replace_null_lf = lf.with_columns(
        pl.when(pl.col(cols).str.to_uppercase().str.contains(na_pattern))  # 대문자 변환 후 패턴 검사
        .then(None)  # 매칭되면 null
        .otherwise(pl.col(cols))  # 매칭 안 되면 원본 유지
        .name.keep()  # 원본 컬럼명 유지
    )
    return replace_null_lf


def yn_to_bool(lf: pl.LazyFrame, cols: List[str]) -> pl.LazyFrame:
    """Y/N 문자열 값을 boolean 타입으로 변환
    
    'Y'는 True로, 'N'은 False로 변환합니다.
    대소문자를 구분하지 않으며, Y/N이 아닌 값은 null이 됩니다.
    
    Parameters:
    -----------
    lf : pl.LazyFrame
        변환할 LazyFrame
    cols : List[str]
        변환할 컬럼명 리스트
    
    Returns:
    --------
    pl.LazyFrame
        Y/N 값이 boolean으로 변환된 LazyFrame
    
    Examples:
    ---------
    >>> # 단일 컬럼 변환
    >>> lf = yn_to_bool(lf, ['report_to_fda'])
    
    >>> # 여러 컬럼 동시 변환
    >>> lf = yn_to_bool(lf, [
    ...     'report_to_fda', 
    ...     'report_to_manufacturer',
    ...     'device_operator_known'
    ... ])
    
    Notes:
    ------
    - 'Y', 'y' → True
    - 'N', 'n' → False  
    - 그 외 값 → None (null)
    - 원본이 이미 null인 경우 null 유지
    """
    bool_lf = lf.with_columns([
        pl.col(col)
        .str.to_uppercase()  # 대소문자 통일 (Y/N으로 변환)
        .replace({'Y': True, 'N': False})  # Y→True, N→False, 나머지→null
        .alias(col)  # 동일한 컬럼명으로 덮어쓰기
        for col in cols
    ])
    return bool_lf


def str_to_categorical(lf: pl.LazyFrame, cols: List[str]) -> pl.LazyFrame:
    """String 타입 컬럼을 Categorical 타입으로 변환
    
    고유값(unique value)이 적은 컬럼을 Categorical로 변환하면:
    - 메모리 사용량 감소 (문자열을 정수 인덱스로 저장)
    - groupby, join 등의 연산 속도 향상
    - 정렬 및 필터링 성능 개선
    
    Parameters:
    -----------
    lf : pl.LazyFrame
        변환할 LazyFrame
    cols : List[str]
        Categorical로 변환할 컬럼명 리스트
    
    Returns:
    --------
    pl.LazyFrame
        지정된 컬럼이 Categorical 타입으로 변환된 LazyFrame
    
    Examples:
    ---------
    >>> # 단일 컬럼 변환
    >>> lf = str_to_categorical(lf, ['device_class'])
    
    >>> # 여러 컬럼 동시 변환 (고유값이 적은 컬럼들)
    >>> lf = str_to_categorical(lf, [
    ...     'device_class',      # 예: 1, 2, 3
    ...     'event_type',        # 예: Injury, Malfunction, Death
    ...     'report_source',     # 예: Manufacturer, User Facility, Distributor
    ...     'country'            # 예: US, CA, UK, JP, ...
    ... ])
    
    >>> # 타입 확인
    >>> lf.collect().schema
    
    Notes:
    ------
    - 고유값이 많은 컬럼(예: ID, 이름)은 변환하지 않는 것이 좋습니다
    - 일반적으로 고유값이 전체 행의 5% 미만일 때 효과적입니다
    - Categorical 타입은 기본적으로 "physical" 순서를 사용합니다
    """
    # 지정된 컬럼들을 Categorical 타입으로 캐스팅
    categorical_lf = lf.with_columns(
        pl.col(cols).cast(pl.Categorical)
    )
    return categorical_lf