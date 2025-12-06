from typing import List, Set, Iterator
import requests
import zipfile
import io
import ijson
from tqdm import tqdm
import time
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json

SEARCH_URL = 'https://api.fda.gov/download.json'


def search_download_url(start: int, end: int) -> List[str]:
    """다운로드 URL 목록 조회"""
    response = requests.get(SEARCH_URL).json()
    partitions = response['results']['device']['event']['partitions']
    
    urls = []
    for item in partitions:
        first = item['display_name'].split()[0]
        if first.isdigit() and start <= int(first) <= end:
            urls.append(item["file"])
    return urls


def stream_records_from_url(url: str) -> Iterator[dict]:
    """URL에서 직접 스트리밍으로 레코드 yield (디스크 사용 없음)"""
    # 1. ZIP 다운로드 (스트리밍)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        
        zip_buffer = io.BytesIO()
        for chunk in tqdm(
            r.iter_content(chunk_size=8 * 1024 * 1024),
            total=total // (8 * 1024 * 1024),
            desc=f"다운로드 {url.split('/')[-1]}",
            leave=False
        ):
            zip_buffer.write(chunk)
        
        zip_buffer.seek(0)
    
    # 2. ZIP 압축 해제 및 JSON 스트리밍
    with zipfile.ZipFile(zip_buffer, 'r') as z:
        json_file = [n for n in z.namelist() if n.endswith(".json")][0]
        
        with z.open(json_file) as f:
            # ijson으로 results 배열의 각 항목을 스트리밍 파싱
            parser = ijson.items(f, 'results.item')
            
            for record in parser:
                yield record
    
    # 메모리 해제
    del zip_buffer


def flatten_dict(nested_dict, parent_key='', sep='_'):
    """중첩된 딕셔너리를 평탄화"""
    items = []
    
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            for i, item in enumerate(v):
                items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
        else:
            items.append((new_key, v))
    
    return dict(items)


def clean_empty_arrays(obj):
    """빈 값 정리"""
    if isinstance(obj, dict):
        return {k: clean_empty_arrays(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        if obj == [""]:
            return None
        return [clean_empty_arrays(item) for item in obj]
    elif obj == "":
        return None
    return obj


def extract_columns_from_record(record: dict) -> Set[str]:
    """단일 레코드에서 컬럼 추출"""
    cleaned = clean_empty_arrays(record)
    flattened = flatten_dict(cleaned)
    return set(flattened.keys())


def normalize_record(record: dict, all_columns: List[str]) -> dict:
    """레코드를 정규화 (모든 컬럼 포함)"""
    cleaned = clean_empty_arrays(record)
    flattened = flatten_dict(cleaned)
    
    normalized = {}
    for col in all_columns:
        val = flattened.get(col, None)
        normalized[col] = str(val) if val is not None else None
    
    return normalized


def pass1_collect_schema(urls: List[str], schema_file: str = '.schema_cache.json') -> List[str]:
    """Pass 1: 모든 URL을 스트리밍하며 전체 스키마 수집"""
    print("\n=== Pass 1: 전체 스키마 수집 ===")
    all_columns = set()
    total_records = 0
    
    for url in urls:
        file_columns = set()
        record_count = 0
        
        print(f"\n📄 처리 중: {url.split('/')[-1]}")
        
        try:
            for record in stream_records_from_url(url):
                columns = extract_columns_from_record(record)
                file_columns.update(columns)
                record_count += 1
                
                # 진행상황 출력 (1000개마다)
                if record_count % 1000 == 0:
                    print(f"  ├─ {record_count:,}개 레코드 스캔...", end='\r')
            
            all_columns.update(file_columns)
            total_records += record_count
            
            print(f"  ✓ {record_count:,}개 레코드, {len(file_columns):,}개 컬럼 발견")
            
        except Exception as e:
            print(f"  ✗ 오류 발생: {e}")
            continue
    
    schema_columns = sorted(all_columns)
    
    # 스키마를 작은 파일로 저장 (재시작 시 재사용 가능)
    with open(schema_file, 'w') as f:
        json.dump(schema_columns, f)
    
    print(f"\n✅ 총 {total_records:,}개 레코드, {len(schema_columns):,}개 고유 컬럼 발견")
    print(f"📝 스키마 저장: {schema_file}\n")
    
    return schema_columns


def pass2_convert_to_parquet(
    urls: List[str], 
    schema_columns: List[str], 
    output_file: str,
    chunk_size: int = 5000
):
    """Pass 2: 스키마 기반으로 Parquet 변환"""
    print("=== Pass 2: Parquet 변환 ===")
    
    schema = pa.schema([(col, pa.string()) for col in schema_columns])
    writer = pq.ParquetWriter(output_file, schema, compression='zstd')
    
    total_written = 0
    buffer = []
    
    for url in urls:
        print(f"\n📄 변환 중: {url.split('/')[-1]}")
        record_count = 0
        
        try:
            for record in stream_records_from_url(url):
                normalized = normalize_record(record, schema_columns)
                buffer.append(normalized)
                record_count += 1
                
                # 버퍼가 차면 쓰기
                if len(buffer) >= chunk_size:
                    table = pa.Table.from_pylist(buffer, schema=schema)
                    writer.write_table(table)
                    total_written += len(buffer)
                    buffer = []
                    
                    print(f"  ├─ {total_written:,}개 레코드 저장...", end='\r')
            
            print(f"  ✓ {record_count:,}개 레코드 완료")
            
        except Exception as e:
            print(f"  ✗ 오류 발생: {e}")
            continue
    
    # 남은 레코드 처리
    if buffer:
        table = pa.Table.from_pylist(buffer, schema=schema)
        writer.write_table(table)
        total_written += len(buffer)
        buffer = []
    
    writer.close()
    
    file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"\n✅ 완료! {total_written:,}개 레코드를 {output_file}에 저장")
    print(f"📦 파일 크기: {file_size_mb:.2f} MB")


def process_fda_data_streaming(
    start: int, 
    end: int, 
    output_file: str = 'output.parquet',
    schema_file: str = '.schema_cache.json',
    skip_pass1: bool = False
):
    """
    전체 프로세스 실행 (완전 스트리밍 버전)
    
    Args:
        start: 시작 연도
        end: 종료 연도
        output_file: 출력 Parquet 파일명
        schema_file: 스키마 캐시 파일명
        skip_pass1: True면 기존 스키마 재사용 (Pass 1 건너뛰기)
    """
    start_time = time.time()
    
    # 1. URL 수집
    print("🔍 다운로드 URL 검색 중...")
    urls = search_download_url(start, end)
    print(f"찾은 URL: {len(urls)}개\n")
    
    if not urls:
        print("❌ 다운로드할 파일이 없습니다.")
        return
    
    # 2. Pass 1: 스키마 수집 (또는 캐시 로드)
    if skip_pass1 and os.path.exists(schema_file):
        print(f"♻️  기존 스키마 로드: {schema_file}")
        with open(schema_file, 'r') as f:
            schema_columns = json.load(f)
        print(f"✅ {len(schema_columns):,}개 컬럼 로드 완료\n")
    else:
        schema_columns = pass1_collect_schema(urls, schema_file)
    
    # 3. Pass 2: Parquet 변환
    pass2_convert_to_parquet(urls, schema_columns, output_file)
    
    total_time = time.time() - start_time
    print(f"\n⏱️  전체 실행 시간: {total_time:.2f}초")


if __name__ == '__main__':   
    # 처음 실행 (Pass 1 + Pass 2)
    process_fda_data_streaming(
        start=2024,
        end=2024,
        output_file='output.parquet',
        skip_pass1=False  # False: 스키마 새로 수집
    )
    
    # 만약 중간에 중단되었다면, 스키마 재사용해서 Pass 2만 실행
    # process_fda_data_streaming(
    #     start=2024,
    #     end=2024,
    #     output_file='output.parquet',
    #     skip_pass1=True  # True: 기존 .schema_cache.json 재사용
    # )