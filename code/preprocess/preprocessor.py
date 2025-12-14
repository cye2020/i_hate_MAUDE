"""
UDI 처리 메인 클래스 (LazyFrame 기반, 클린징 제거)
"""
import polars as pl
from pathlib import Path
# from pprint import pprint
from code.preprocess.config import Config
from code.preprocess.preprocess import (
    extract_di_from_public, 
    fuzzy_match_dict, 
    collect_unique_safe
)
from code.utils.chunk import process_lazyframe_in_chunks


class UDIProcessor:
    """UDI-DI 결측 처리 클래스 (LazyFrame 최적화, 클린징된 데이터 입력)"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.udi_di_lookup = None
        self.mfr_lookup_full = None
        self.mfr_lookup_partial = None
        self.mfr_mapping = None
    
    # ==================== 1단계: 전처리 (LazyFrame 유지) ====================
    
    def preprocess_maude(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        MAUDE 전처리 (LazyFrame 유지)
        - 이미 클린징된 데이터를 받음
        - manufacturer_std, brand는 이미 정규화됨
        
        Returns:
            LazyFrame (collect 안 함)
        """
        print("🔧 MAUDE 전처리...")
        
        total_cols = lf.collect_schema().names()
        
        # 1. UDI-Public → DI 추출
        result_lf = lf.with_columns([
            pl.col('udi_public')
              .map_elements(extract_di_from_public, return_dtype=pl.Utf8)
              .alias('extracted_di'),
            
            # 2. 날짜 통합
            pl.coalesce([pl.col(c) for c in self.config.MAUDE_DATES if c in total_cols])
              .alias('report_date'),
        ])
        
        # 3. UDI 통합
        result_lf = result_lf.with_columns([
            pl.coalesce(['udi_di', 'extracted_di']).alias('udi_combined'),
            
            pl.when(pl.col('udi_di').is_not_null())
              .then(pl.lit('original'))
              .when(pl.col('extracted_di').is_not_null())
              .then(pl.lit('extracted'))
              .otherwise(pl.lit('missing'))
              .alias('udi_source')
        ])
        
        print(f"   ✓ 전처리 완료 (LazyFrame 유지)")
        return result_lf
    
    def preprocess_udi_db(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        UDI DB 전처리 (LazyFrame 유지)
        - 이미 클린징된 데이터를 받음
        """
        print("🔧 UDI DB 전처리...")
        
        total_cols = lf.collect_schema().names()
        # 날짜 통합만
        return lf.with_columns([
            pl.coalesce([pl.col(c) for c in self.config.UDI_DATES if c in total_cols])
              .alias('publish_date')
        ])
    
    def normalize_manufacturers(self, maude_lf: pl.LazyFrame, udi_lf: pl.LazyFrame):
        """
        제조사명 퍼지 매칭 (Unique만 collect - 안전)
        - manufacturer_std는 이미 클린징됨
        """
        print("🔧 제조사명 퍼지 매칭...")
        
        # Unique만 collect (수천 개 수준 - 안전)
        maude_mfrs = collect_unique_safe(maude_lf, 'manufacturer')
        udi_mfrs = collect_unique_safe(udi_lf, 'manufacturer')
        
        self.mfr_mapping = fuzzy_match_dict(
            maude_mfrs, 
            udi_mfrs, 
            self.config.FUZZY_THRESHOLD
        )
        
        # pprint([(k, v) for k,v in self.mfr_mapping.items() if k!=v])
        
        print(f"   매칭: {sum(k!=v for k,v in self.mfr_mapping.items())}/{len(maude_mfrs)} 건")
    
    def apply_normalization(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """제조사명 정규화 적용 (LazyFrame 유지)"""
        return lf.with_columns([
            pl.col('manufacturer').replace(self.mfr_mapping).alias('mfr_std')
        ])
    
    # ==================== 2단계: Lookup (collect 필요) ====================

    def build_lookup(self, udi_lf: pl.LazyFrame):
        """
        Lookup 테이블 생성 (Primary만, Secondary는 별도 처리)
        """
        print("🔧 Lookup 테이블 생성...")
        
        # ========== Lookup 1: Primary UDI-DI만 ==========
        self.udi_di_lookup = udi_lf.select([
            'udi_di',
            'manufacturer',
            'brand',
            'model_number',
            'catalog_number',
            'publish_date'
        ]).unique(subset=['udi_di']).collect()
        
        print(f"   UDI-DI Lookup: {len(self.udi_di_lookup):,} 건")
        
        # ========== Lookup 2: Full key ==========
        self.mfr_lookup_full = udi_lf.group_by([
            'manufacturer', 'brand', 'catalog_number'
        ]).agg([
            pl.col('udi_di').len().alias('n_versions_full'),
            pl.col('udi_di').alias('udi_list_full'),
            pl.col('model_number').alias('model_list_full'),
            pl.col('publish_date').alias('date_list_full')
        ]).collect()
        
        print(f"   제조사 Full Lookup: {len(self.mfr_lookup_full):,} 건")
        
        # ========== Lookup 3: Partial key ==========
        self.mfr_lookup_partial = udi_lf.group_by([
            'manufacturer', 'brand'
        ]).agg([
            pl.col('udi_di').len().alias('n_versions_partial'),
            pl.col('udi_di').alias('udi_list_partial'),
            pl.col('catalog_number').alias('catalog_list_partial'),
            pl.col('model_number').alias('model_list_partial'),
            pl.col('publish_date').alias('date_list_partial')
        ]).collect()
        
        print(f"   제조사 Partial Lookup: {len(self.mfr_lookup_partial):,} 건")
        
    # ==================== 3-5단계: 매칭/해결 (chunk 처리) ====================
    
    def process_all(
        self,
        maude_lf: pl.LazyFrame,
        output_path: Path,
        chunk_size: int = 1_000_000
    ):
        """
        전체 파이프라인 (UDI 매핑 테이블 활용)
        """
        print("\n🔧 매칭 단계 (UDI 매핑 + 제조사 매칭)...")
        
        def transform_chunk(chunk_lf: pl.LazyFrame) -> pl.LazyFrame:
            # ========== Step 1: UDI 매핑 테이블 join (작고 빠름!) ==========
            matched = chunk_lf.join(
                self.udi_mapping.lazy(),
                on='udi_combined',
                how='left'
            )
            
            # ========== Step 2: Full key 매칭 ==========
            matched = matched.join(
                self.mfr_lookup_full.lazy(),
                left_on=['mfr_std', 'brand', 'catalog_number'],
                right_on=['manufacturer', 'brand', 'catalog_number'],
                how='left',
                suffix='_full'
            )
            
            # ========== Step 3: Partial key 매칭 ==========
            matched = matched.join(
                self.mfr_lookup_partial.lazy(),
                left_on=['mfr_std', 'brand'],
                right_on=['manufacturer', 'brand'],
                how='left',
                suffix='_partial'
            )
            
            # ========== Step 4: 단일 매칭만 성공 ==========
            matched = matched.with_columns([
                pl.when(pl.col('n_versions_full') == 1)
                .then(pl.col('udi_list_full').list.first())
                .otherwise(None)
                .alias('matched_udi_full'),
                
                pl.when(pl.col('n_versions_full') == 1)
                .then(pl.col('model_list_full').list.first())
                .otherwise(None)
                .alias('matched_model_full'),
                
                pl.when(pl.col('n_versions_partial') == 1)
                .then(pl.col('udi_list_partial').list.first())
                .otherwise(None)
                .alias('matched_udi_partial'),
                
                pl.when(pl.col('n_versions_partial') == 1)
                .then(pl.col('catalog_list_partial').list.first())
                .otherwise(None)
                .alias('matched_catalog_partial'),
                
                pl.when(pl.col('n_versions_partial') == 1)
                .then(pl.col('model_list_partial').list.first())
                .otherwise(None)
                .alias('matched_model_partial'),
            ])
            
            # ========== Step 5: 우선순위 통합 ==========
            matched = matched.with_columns([
                # device_version_id: UDI 매핑 우선
                pl.coalesce([
                    'mapped_primary_udi',      # UDI 매핑 (direct/secondary)
                    'matched_udi_full',        # 제조사 Full (단일)
                    'matched_udi_partial',     # 제조사 Partial (단일)
                    'udi_combined',            # Fallback
                ]).alias('device_version_id'),
                
                # manufacturer: UDI 매핑 우선
                pl.coalesce([
                    'mapped_manufacturer',
                    'manufacturer',
                ]).alias('manufacturer_final'),
                
                # brand
                pl.coalesce([
                    'mapped_brand',
                    'brand',
                ]).alias('brand_final'),
                
                # model_number
                pl.coalesce([
                    'mapped_model_number',
                    'matched_model_full',
                    'matched_model_partial',
                ]).alias('model_number_final'),
                
                # catalog_number
                pl.coalesce([
                    'mapped_catalog_number',
                    'catalog_number',
                    'matched_catalog_partial',
                ]).alias('catalog_number_final'),
                
                # match_source
                pl.when(pl.col('udi_match_type') == 'udi_direct')
                .then(pl.lit('udi_direct'))
                .when(pl.col('udi_match_type') == 'udi_secondary')
                .then(pl.lit('udi_secondary'))
                .when(pl.col('matched_udi_full').is_not_null())
                .then(pl.lit('mfr_full_single'))
                .when(pl.col('matched_udi_partial').is_not_null())
                .then(pl.lit('mfr_partial_single'))
                .when(pl.col('n_versions_full') > 1)
                .then(pl.lit('mfr_full_multiple'))
                .when(pl.col('n_versions_partial') > 1)
                .then(pl.lit('mfr_partial_multiple'))
                .when(pl.col('udi_match_type') == 'udi_no_match')
                .then(pl.lit('udi_no_match'))
                .otherwise(pl.lit('no_match'))
                .alias('match_source')
            ])
            
            # 컬럼 선택
            original_cols = chunk_lf.collect_schema().names()
            final_cols = [
                *original_cols,
                'device_version_id',
                'manufacturer_final',
                'brand_final',
                'model_number_final',
                'catalog_number_final',
                'match_source',
            ]
            
            return matched.select([c for c in final_cols if c in matched.collect_schema().names()])
        
        process_lazyframe_in_chunks(
            lf=maude_lf,
            transform_func=transform_chunk,
            output_path=output_path,
            chunk_size=chunk_size,
            desc="UDI 매핑 + 제조사 매칭"
        )
    
    def _post_process_complex_cases(self, input_path: Path, chunk_size: int):
        """후처리 - 다중 매칭과 Tier 3 처리"""
        print("\n🔧 후처리 (다중 매칭 & Tier 3)...")
        
        lf = pl.scan_parquet(input_path)
        
        # 제조사별 준수율
        compliance = lf.group_by('mfr_std').agg([
            (pl.col('udi_combined').is_null().sum() / pl.len()).alias('missing_rate')
        ]).collect()
        
        low_compliance_mfrs = compliance.filter(
            pl.col('missing_rate') > self.config.LOW_COMPLIANCE_THRESHOLD
        )['mfr_std'].to_list()
        
        def resolve_chunk(chunk_lf: pl.LazyFrame) -> pl.LazyFrame:
            # 다중 매칭 + no_match → Tier 3 처리
            chunk_lf = chunk_lf.with_columns([
                pl.when(
                    pl.col('match_source').is_in([
                        'mfr_full_multiple',
                        'mfr_partial_multiple',
                        'no_match'
                    ])
                )
                .then(
                    # UDI 있으면 그대로 사용 (정보 없어도 UDI는 살림)
                    pl.when(pl.col('udi_combined').is_not_null())
                      .then(pl.col('udi_combined'))
                      # UDI 없으면 Tier 3 ID 생성
                      .when(pl.col('mfr_std').is_in(low_compliance_mfrs))
                      .then(pl.concat_str([
                          pl.lit('LOW_'), pl.col('mfr_std'), pl.lit('_'), pl.col('brand_final')
                      ]))
                      .otherwise(pl.concat_str([
                          pl.lit('UNK_'), pl.col('mfr_std'), pl.lit('_'), 
                          pl.col('brand_final'), pl.lit('_'), pl.col('catalog_number_final')
                      ]))
                )
                .otherwise(pl.col('device_version_id'))
                .alias('device_version_id'),
                
                # 신뢰도
                pl.when(pl.col('match_source') == 'udi_direct')
                  .then(pl.lit('HIGH'))
                  .when(pl.col('match_source').str.contains('single'))
                  .then(pl.lit('MEDIUM'))
                  .when(pl.col('match_source').str.contains('multiple'))
                  .then(pl.lit('LOW'))  # 다중 매칭 실패
                  .when(pl.col('udi_combined').is_not_null())
                  .then(pl.lit('MEDIUM'))  # UDI 있지만 정보 없음
                  .otherwise(pl.lit('VERY_LOW'))
                  .alias('udi_confidence'),
                
                pl.col('match_source').alias('final_source')
            ])
            
            return chunk_lf
        
        output_path = input_path.parent / f"{input_path.stem}_resolved.parquet"
        
        process_lazyframe_in_chunks(
            lf=lf,
            transform_func=resolve_chunk,
            output_path=output_path,
            chunk_size=chunk_size,
            desc="다중 매칭 & Tier 3 처리"
        )
        
        print(f"✅ 최종 결과: {output_path}")
        return output_path
    
    # ==================== 전체 실행 ====================

    def process(
        self,
        maude_lf: pl.LazyFrame,
        udi_lf: pl.LazyFrame,
        output_path: Path,
        chunk_size: int = 50_000
    ) -> Path:
        """전체 파이프라인"""
        print("="*60)
        print("UDI 처리 파이프라인 시작 (효율적 매핑)")
        print("="*60)
        
        # 1. 전처리
        maude_lf = self.preprocess_maude(maude_lf)
        udi_lf = self.preprocess_udi_db(udi_lf)
        
        # 2. 제조사명 정규화
        self.normalize_manufacturers(maude_lf, udi_lf)
        maude_lf = self.apply_normalization(maude_lf)
        
        # 3. Lookup 생성 (Primary + 제조사)
        self.build_lookup(udi_lf)
        
        # 4. UDI 매핑 테이블 생성 (Primary + Secondary) ← 신규!
        self.build_udi_mapping(maude_lf, udi_lf, chunk_size)
        
        # 5-6. 매칭/해결 (chunk)
        temp_path = output_path.parent / f"{output_path.stem}_temp.parquet"
        self.process_all(maude_lf, temp_path, chunk_size)
        
        # 7. 후처리
        final_path = self._post_process_complex_cases(temp_path, chunk_size)
        
        # 8. 최종 파일 이동
        final_path.rename(output_path)
        temp_path.unlink(missing_ok=True)
        
        # 통계
        print("\n" + "="*60)
        print("📊 최종 결과")
        print("="*60)
        
        result_lf = pl.scan_parquet(output_path)
        total = result_lf.select(pl.len()).collect().item()
        
        match_stats = result_lf.group_by('match_source').agg([
            pl.len().alias('count'),
            (pl.len() / total * 100).round(2).alias('percent')
        ]).collect().sort('count', descending=True)
        
        print("\n매칭 출처 분포:")
        print(match_stats)
        
        print(f"\n✅ 총 {total:,} 건 처리 완료!")
        print(f"📁 결과: {output_path}")
        
        return output_path

    def build_udi_mapping(self, maude_lf: pl.LazyFrame, udi_lf: pl.LazyFrame, chunk_size: int = 50_000):
        """
        UDI 매핑 테이블 생성 (Secondary는 chunk 처리)
        """
        print("🔧 UDI 매핑 테이블 생성...")
        
        # ========== Step 1: Unique UDI 추출 ==========
        unique_udi = maude_lf.select([
            'udi_combined'
        ]).unique().filter(
            pl.col('udi_combined').is_not_null()
        ).collect()
        
        print(f"   Unique UDI: {len(unique_udi):,} 건")
        
        # ========== Step 2: Primary 매칭 ==========
        udi_with_primary = unique_udi.lazy().join(
            self.udi_di_lookup.lazy(),
            left_on='udi_combined',
            right_on='udi_di',
            how='left',
        ).with_columns([
            pl.col('manufacturer').is_not_null().alias('primary_matched')
        ])
        
        # Primary 성공/실패 분리 (collect - 작음)
        primary_success = udi_with_primary.filter(pl.col('primary_matched')).collect()
        primary_failed = udi_with_primary.filter(~pl.col('primary_matched')).collect()
        
        print(f"   - Primary 매칭 성공: {len(primary_success):,} 건")
        print(f"   - Primary 매칭 실패: {len(primary_failed):,} 건")
        
        # ========== Step 3: Secondary 매칭 (chunk 처리!) ==========
        schema = udi_lf.collect_schema()
        secondary_cols = [c for c in schema.names() if c.startswith('identifiers_') and c.endswith('_id')]
        
        if secondary_cols and len(primary_failed) > 0:
            print(f"   Secondary 매칭 시도 중... ({len(secondary_cols)}개 컬럼)")
            
            # Primary 실패한 UDI 리스트
            failed_udi_list = primary_failed['udi_combined'].to_list()
            failed_udi_set = set(failed_udi_list)  # 빠른 lookup용
            
            print(f"   매칭 대상 UDI: {len(failed_udi_set):,} 건")
            
            # ========== UDI DB를 chunk로 처리 ==========
            def build_secondary_mapping_chunk(chunk_lf: pl.LazyFrame) -> pl.LazyFrame:
                """각 chunk에서 secondary 매칭 찾기"""
                # concat_list로 secondary 컬럼 합치기
                chunk_with_list = chunk_lf.with_columns([
                    pl.concat_list(secondary_cols).alias('secondary_list')
                ]).select([
                    'udi_di',
                    'manufacturer',
                    'brand',
                    'model_number',
                    'catalog_number',
                    'secondary_list'
                ])
                
                # explode (chunk 단위라 안전)
                exploded = chunk_with_list.explode('secondary_list').filter(
                    pl.col('secondary_list').is_not_null()
                )
                
                # Primary 실패한 UDI와 매칭되는 것만 필터
                matched = exploded.filter(
                    pl.col('secondary_list').is_in(failed_udi_list)
                )
                
                # group_by
                return matched.group_by('secondary_list').agg([
                    pl.col('udi_di').n_unique().alias('n_primary'),
                    pl.col('udi_di').first().alias('primary_udi'),
                    pl.col('manufacturer').first().alias('manufacturer'),
                    pl.col('brand').first().alias('brand'),
                    pl.col('model_number').first().alias('model_number'),
                    pl.col('catalog_number').first().alias('catalog_number'),
                ])
            
            # chunk 처리
            temp_secondary_path = Path("data/temp_secondary_mapping.parquet")
            
            process_lazyframe_in_chunks(
                lf=udi_lf,
                transform_func=build_secondary_mapping_chunk,
                output_path=temp_secondary_path,
                chunk_size=chunk_size,  # UDI DB chunk 크기
                desc="Secondary 매핑 생성"
            )
            
            # 결과 로드 & 통합 (중복 제거)
            secondary_mapping_all = pl.scan_parquet(temp_secondary_path).group_by(
                'secondary_list'
            ).agg([
                pl.col('n_primary').sum().alias('n_primary'),  # chunk 간 합산
                pl.col('primary_udi').first().alias('primary_udi'),
                pl.col('manufacturer').first().alias('manufacturer'),
                pl.col('brand').first().alias('brand'),
                pl.col('model_number').first().alias('model_number'),
                pl.col('catalog_number').first().alias('catalog_number'),
            ]).collect()
            
            print(f"   - Secondary 매핑 생성 완료: {len(secondary_mapping_all):,} 건")
            
            # Primary 실패한 UDI와 join
            secondary_matched = primary_failed.lazy().join(
                secondary_mapping_all.lazy(),
                left_on='udi_combined',
                right_on='secondary_list',
                how='left'
            ).with_columns([
                (pl.col('n_primary') == 1).alias('secondary_matched')
            ]).collect()
            
            # 임시 파일 삭제
            temp_secondary_path.unlink(missing_ok=True)
            
            secondary_success_count = secondary_matched.filter(
                pl.col('secondary_matched')
            ).shape[0]
            
            print(f"   - Secondary 매칭 성공: {secondary_success_count:,} 건 (단일 Primary)")
            print(f"   - Secondary 매칭 실패: {len(primary_failed) - secondary_success_count:,} 건")
            
        else:
            print("   ⚠️  Secondary 매칭 Skip")
            secondary_matched = primary_failed.with_columns([
                pl.lit(False).alias('secondary_matched'),
                pl.lit(None).cast(pl.Utf8).alias('primary_udi'),
                pl.lit(None).cast(pl.Utf8).alias('manufacturer'),
                pl.lit(None).cast(pl.Utf8).alias('brand'),
                pl.lit(None).cast(pl.Utf8).alias('model_number'),
                pl.lit(None).cast(pl.Utf8).alias('catalog_number'),
            ])
        
        # ========== Step 4: 매핑 테이블 통합 ==========
        # Primary 성공
        primary_mapping = primary_success.select([
            'udi_combined',
            pl.col('udi_combined').alias('mapped_primary_udi'),
            pl.col('manufacturer').alias('mapped_manufacturer'),
            pl.col('brand').alias('mapped_brand'),
            pl.col('model_number').alias('mapped_model_number'),
            pl.col('catalog_number').alias('mapped_catalog_number'),
            pl.lit('udi_direct').alias('udi_match_type')
        ])
        
        # Secondary 성공
        secondary_success_mapping = secondary_matched.filter(
            pl.col('secondary_matched')
        ).select([
            'udi_combined',
            pl.col('primary_udi').alias('mapped_primary_udi'),
            pl.col('manufacturer').alias('mapped_manufacturer'),
            pl.col('brand').alias('mapped_brand'),
            pl.col('model_number').alias('mapped_model_number'),
            pl.col('catalog_number').alias('mapped_catalog_number'),
            pl.lit('udi_secondary').alias('udi_match_type')
        ])
        
        # Secondary 실패
        secondary_failed_mapping = secondary_matched.filter(
            ~pl.col('secondary_matched')
        ).select([
            'udi_combined',
            pl.col('udi_combined').alias('mapped_primary_udi'),
            pl.lit(None).cast(pl.Utf8).alias('mapped_manufacturer'),
            pl.lit(None).cast(pl.Utf8).alias('mapped_brand'),
            pl.lit(None).cast(pl.Utf8).alias('mapped_model_number'),
            pl.lit(None).cast(pl.Utf8).alias('mapped_catalog_number'),
            pl.lit('udi_no_match').alias('udi_match_type')
        ])
        
        # 통합
        self.udi_mapping = pl.concat([
            primary_mapping,
            secondary_success_mapping,
            secondary_failed_mapping
        ])
        
        print(f"   ✅ 최종 UDI 매핑: {len(self.udi_mapping):,} 건")
        print(f"      - udi_direct: {(self.udi_mapping['udi_match_type']=='udi_direct').sum():,}")
        print(f"      - udi_secondary: {(self.udi_mapping['udi_match_type']=='udi_secondary').sum():,}")
        print(f"      - udi_no_match: {(self.udi_mapping['udi_match_type']=='udi_no_match').sum():,}")