import polars as pl
from pathlib import Path
from typing import Dict, Optional
import logging
from datetime import datetime

from src.preprocess2.config import get_config
from src.preprocess2.steps import ColumnDropStage, FilterStage
logger = logging.getLogger(__name__)


class SilverPipeline:
    def __init__(self, use_s3: bool = False, datasets: list = None):
        """
        Args:
            use_s3: S3 사용 여부
            datasets: 처리할 데이터셋 리스트 ['maude', 'udi'] (기본값: ['maude'])
        """
        self.config = get_config()
        self.drop_columns_1st_config: dict = self.config.columns.get('column_drop_1st')
        self.scope_config: dict = self.config.filtering.get('scoping')
        
        self.use_s3 = use_s3 if use_s3 is not None else self.config.base['paths'].get('use_s3', False)
        self.datasets = datasets or ['maude', 'udi']
        
        # 데이터셋별 경로 저장
        self.paths = {
            'bronze': {},
            'silver': {}
        }
        
        for dataset in self.datasets:
            self.paths['bronze'][dataset] = self._get_path('bronze', dataset)
            self.paths['silver'][dataset] = self._get_path('silver', dataset)
        
        self.stats = {}
        
        logger.info(f"SilverPipeline initialized for datasets: {self.datasets}")
        for dataset in self.datasets:
            logger.info(f"  {dataset.upper()}")
            logger.info(f"    Bronze: {self.paths['bronze'][dataset]}")
            logger.info(f"    Silver: {self.paths['silver'][dataset]}")
    
    def _get_path(self, stage: str, dataset: str) -> Path:
        """경로 가져오기 (통합)"""
        if self.use_s3:
            base = self.config.storage['s3']['paths'][stage]
        else:
            base = self.config.base['paths']['local'][stage]
        
        filename = self.config.base['datasets'][dataset][f'{stage}_file']
        return Path(base) / filename
    
    def load_bronze_data(self, dataset: str = 'maude') -> pl.LazyFrame:
        """Bronze 데이터 로드"""
        if dataset not in self.datasets:
            raise ValueError(f"Dataset '{dataset}' not configured in pipeline")
        
        data_path = self.paths['bronze'][dataset]
        
        logger.info(f"Loading {dataset} bronze data from {data_path}...")
        
        # 파일 존재 확인 (로컬인 경우만)
        if not self.use_s3 and not data_path.exists():
            raise FileNotFoundError(
                f"Bronze data not found: {data_path}\n"
                f"Please run the bronze pipeline first."
            )
        
        try:
            lf = pl.scan_parquet(str(data_path))
            
            schema = lf.collect_schema()
            col_count = schema.len()
            
            logger.info(f"  ✓ Loaded {dataset} data ({col_count} columns)")
            
            return lf
            
        except Exception as e:
            logger.error(f"Failed to load {dataset} bronze data: {e}")
            raise

    def validate_bronze_data(self, lf: pl.LazyFrame) -> bool:
        """
        Bronze 데이터 기본 검증

        Status:
            Planned (Not yet used in production)

        Notes:
            - 현재 파이프라인에서는 호출되지 않는다.
            - 스키마 안정화 이후 활성화 예정.
        """
        if not self.config.pipeline['validation']['bronze']['enabled']:
            raise RuntimeError("Bronze validation is not enabled yet")
        
        logger.info("Validating bronze data...")
        
        schema = lf.collect_schema()
        
        # 필수 컬럼 확인
        required_columns = self.config.quality['critical_fields']['fields']
        missing_columns = [col for col in required_columns if col not in schema.names()]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # 최소 행 개수 확인 (실제 collect 필요)
        min_rows = self.config.pipeline['validation']['silver']['min_rows']
        row_count = lf.select(pl.len()).collect().item()
        
        if row_count < min_rows:
            logger.error(f"Insufficient rows: {row_count} < {min_rows}")
            return False
        
        self.stats['bronze_rows'] = row_count
        logger.info(f"  ✓ Bronze data validated")
        logger.info(f"  ✓ Rows: {row_count:,}")
        
        return True
    
    def run(self) -> bool:
        """Silver 파이프라인 전체 실행
        
        Returns:
            성공 여부
        """
        try:
            self.stats['start_time'] = datetime.now()
            logger.info("=" * 60)
            logger.info("Starting Silver Pipeline")
            logger.info("=" * 60)
            
            # 1. Bronze 데이터 로드
            maude_lf = self.load_bronze_data('maude')
            udi_lf = self.load_bronze_data('udi')
            
            # 2. 검증 단계 (TODO: 구현 필요)
            
            # 3. 전처리 단계들 (TODO: 구현 필요)
            
            # 3-1. 1차 컬럼 드랍
            logger.info('Drop MAUDE Columns...')
            maude_lf = self.drop_columns_1st(maude_lf, 'maude')
            logger.info('Drop UDI Columns...')
            udi_lf = self.drop_columns_1st(udi_lf, 'udi')
            
            # 3-2. 스코핑 (device class 3만 선택, mdr_text 존재)
            logger.info('Scoping MAUDE Data...')
            maude_lf = self.scope_data(maude_lf, 'maude')
            logger.info('Scoping UDI Data...')
            udi_lf = self.scope_data(udi_lf, 'udi')
            
            # 3-2. 클렌징
            # maude_clean_lf = self.clean_columns(maude_lf, 'maude')
            # udi_clean_lf = self.clean_columns(udi_lf, 'udi')
            
            # lf = self.clean_data(lf)
            # lf = self.remove_duplicates(lf)
            
            # 4. Silver 데이터 저장
            # self.save_silver_data(lf)
            
            self.stats['end_time'] = datetime.now()
            self._log_summary()
            
            logger.info("Silver pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Silver pipeline failed: {e}", exc_info=True)
            return False
    
    def drop_columns_1st(self, lf: pl.LazyFrame, dataset: str) -> pl.LazyFrame:
        verbose = self.drop_columns_1st_config.get('verbose')
        drop_config: dict = self.drop_columns_1st_config.get(dataset)
        
        mode = drop_config.get('mode', 'blacklist')
        patterns = drop_config.get('patterns', [])
        cols = drop_config.get('cols', [])
        
        drop_stage = ColumnDropStage(verbose)
        dropped_lf = drop_stage.drop_columns(lf, patterns, cols, mode)
        
        return dropped_lf

    def scope_data(self, lf: pl.LazyFrame, dataset: str) -> pl.LazyFrame:
        verbose = self.scope_config.get('verbose')
        scope_config: dict = self.scope_config.get(dataset)
        groups: Dict[list] = scope_config.get('groups', [])
        combine_groups = scope_config.get('combine_groups', 'AND')
        
        filter_stage = FilterStage(verbose)
        scope_lf = filter_stage.filter_groups(lf, groups, combine_groups)
        
        return scope_lf

    def _log_summary(self):
        """처리 결과 요약 로그"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info("=" * 60)
        logger.info("Silver Pipeline Summary")
        logger.info("=" * 60)
        logger.info(f"Duration: {duration:.2f} seconds")
        # logger.info(f"Bronze rows: {self.stats['bronze_rows']:,}")
        # logger.info(f"Silver rows: {self.stats['silver_rows']:,}")
        # logger.info(f"Columns dropped: {len(self.stats['columns_dropped'])}")
        # logger.info(f"Duplicates removed: {self.stats['duplicates_removed']:,}")
        logger.info("=" * 60)


# 사용 예시
if __name__ == '__main__':
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s'
    )
    
    # 파이프라인 실행
    pipeline = SilverPipeline(use_s3=False, datasets=['maude', 'udi'])
    success = pipeline.run()
    
    # if success:
    #     print("✓ Pipeline completed successfully")
    # else:
    #     print("✗ Pipeline failed")