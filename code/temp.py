import pyarrow as pa
import pyarrow.parquet as pq
from download import search_and_collect_json

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
    """빈 문자열만 있는 배열을 None으로 변환"""
    if isinstance(obj, dict):
        return {k: clean_empty_arrays(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        if obj == [""]:
            return None
        return [clean_empty_arrays(item) for item in obj]
    elif obj == "":
        return None
    return obj

def collect_all_columns_from_list(records_list):
    """Pass 1: 레코드 리스트에서 모든 컬럼 수집"""
    print("=== Pass 1: 모든 컬럼 수집 중 ===")
    all_columns = set()
    
    for i, record in enumerate(records_list):
        cleaned = clean_empty_arrays(record)
        flattened = flatten_dict(cleaned)
        all_columns.update(flattened.keys())
        
        if (i + 1) % 1000 == 0:
            print(f"{i + 1:,}개 레코드 스캔, {len(all_columns):,}개 컬럼 발견...")
    
    print(f"\n총 {len(records_list):,}개 레코드, {len(all_columns):,}개 고유 컬럼 발견")
    return sorted(all_columns)

def normalize_record(record, all_columns):
    """레코드를 통일된 컬럼 구조로 정규화"""
    normalized = {}
    for col in all_columns:
        normalized[col] = record.get(col, None)
    return normalized

def dict_to_parquet_streaming(records_generator, parquet_file, chunk_size=5000):
    # Pass 1: 모든 컬럼 수집
    print("=== Pass 1: 모든 컬럼 수집 및 타입 샘플링 중 ===")
    all_columns = set()
    temp_records = []
    record_count = 0
    
    for record in records_generator:
        cleaned = clean_empty_arrays(record)
        flattened = flatten_dict(cleaned)
        all_columns.update(flattened.keys())
        temp_records.append(record)
        record_count += 1
        
        if record_count % 1000 == 0:
            print(f"{record_count:,}개 레코드 스캔...")
    
    all_columns = sorted(all_columns)
    print(f"\n총 {record_count:,}개 레코드, {len(all_columns):,}개 컬럼 발견")
    
    # 스키마 정의: 모든 컬럼을 string으로 통일 (가장 안전)
    schema = pa.schema([(col, pa.string()) for col in all_columns])
    print(f"스키마 생성 완료: {len(schema)} 컬럼")
    
    # Pass 2: 변환
    print("\n=== Pass 2: Parquet 변환 중 ===")
    records_buffer = []
    writer = pq.ParquetWriter(parquet_file, schema, compression='zstd')  # 미리 writer 생성
    total_processed = 0
    
    for record in temp_records:
        cleaned = clean_empty_arrays(record)
        flattened = flatten_dict(cleaned)
        normalized = normalize_record(flattened, all_columns)
        
        # 모든 값을 문자열로 변환 (None은 유지)
        normalized = {k: (str(v) if v is not None else None) for k, v in normalized.items()}
        records_buffer.append(normalized)
        
        if len(records_buffer) >= chunk_size:
            table = pa.Table.from_pylist(records_buffer, schema=schema)  # 스키마 명시
            writer.write_table(table)
            total_processed += len(records_buffer)
            print(f"{total_processed:,}개 처리...")
            records_buffer = []
    
    # 남은 레코드
    if records_buffer:
        table = pa.Table.from_pylist(records_buffer, schema=schema)
        writer.write_table(table)
        total_processed += len(records_buffer)
    
    writer.close()
    print(f"\n완료! {total_processed:,}개 레코드 저장")

# 사용 예시
def record_generator(data_dict):
    """dict에서 레코드를 하나씩 yield"""
    for record in data_dict['results']:
        yield record

if __name__=='__main__':
    start, end = 2024, 2024
    data = search_and_collect_json(start, end)

    # import json
    
    # # JSON 파일에서 dict 로드
    # print("JSON 파일 읽는 중...")
    # with open('sample.json', 'r') as f:
    #     data = json.load(f)
    dict_to_parquet_streaming(record_generator(data), 'output.parquet')