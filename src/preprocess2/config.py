# src/preprocess/config.py (전처리 전용 - 고수준)
from pathlib import Path
from typing import Dict, Any, List, Optional
from config.config_loader import load_config

class PreprocessConfig:
    """전처리 설정 통합 관리 클래스
    
    IDE 자동완성을 지원하며 타입 안전성을 제공합니다.
    """
    
    def __init__(self):
        # 기본 설정들 (캐싱됨)
        self._base = load_config("base")
        self._storage = load_config("storage")
        self._pipeline = load_config("pipeline")
        
        # 전처리 설정들 (캐싱됨)
        self._columns = load_config("preprocess/columns")
        self._filtering = load_config("preprocess/filtering")
        self._cleaning = load_config("preprocess/cleaning")
        self._deduplication = load_config("preprocess/deduplication")
        self._udi_matching = load_config("preprocess/udi_matching")
        self._transformation = load_config("preprocess/transformation")
    
    # ==================== 기본 설정 ====================
    
    @property
    def base(self) -> Dict[Any, Any]:
        """기본 설정"""
        return self._base
    
    @property
    def storage(self) -> Dict[Any, Any]:
        """저장소 설정 (S3, Snowflake)"""
        return self._storage
    
    @property
    def pipeline(self) -> Dict[Any, Any]:
        """파이프라인 설정"""
        return self._pipeline
    
    # ==================== 전처리 설정 ====================
    
    @property
    def cleaning(self) -> Dict[Any, Any]:
        """클린징 설정"""
        return self._cleaning
    
    @property
    def deduplication(self) -> Dict[Any, Any]:
        """중복 제거 설정"""
        return self._deduplication
    
    @property
    def udi_matching(self) -> Dict[Any, Any]:
        """UDI 매칭 설정"""
        return self._udi_matching
    
    @property
    def filtering(self) -> Dict[Any, Any]:
        """품질 필터링 설정"""
        return self._filtering
    
    @property
    def transformation(self) -> Dict[Any, Any]:
        """타입 변환 설정"""
        return self._transformation
    
    @property
    def columns(self) -> Dict[Any, Any]:
        """컬럼 선택/제거 설정"""
        return self._columns
    
    # ==================== 편의 메서드 ====================
    
    def get_na_patterns(self) -> List[str]:
        """NA 패턴 목록 반환"""
        return self._cleaning['na_patterns']['patterns']
    
    def get_dedup_columns(self) -> List[str]:
        """중복 제거 기준 컬럼 반환"""
        return self._deduplication['maude']['subset_columns']
    
    def get_udi_strategies(self) -> List[Dict]:
        """UDI 매칭 전략 반환"""
        return self._udi_matching['strategies']
    
    def get_drop_patterns(self, stage: str = '1st') -> List[str]:
        """컬럼 Drop 패턴 반환
        
        Args:
            stage: '1st' 또는 '2nd'
        """
        key = f'column_drop_{stage}'
        return self._columns[key]['drop_patterns']
    
    def get_final_columns(self) -> List[str]:
        """최종 유지할 컬럼 목록 반환"""
        return self._columns['column_drop_2nd']['keep_columns']
    
    def get_fuzzy_threshold(self) -> int:
        """퍼지 매칭 임계값 반환"""
        return self._udi_matching['fuzzy_matching']['threshold']
    
    def is_enabled(self, feature: str) -> bool:
        """특정 기능 활성화 여부 확인
        
        Args:
            feature: 'deduplication', 'udi_matching' 등
        """
        config_map = {
            'deduplication': self._deduplication['maude']['enabled'],
            'udi_matching': self._udi_matching['strategies'][0]['enabled'],
        }
        return config_map.get(feature, False)
    
    # ==================== 경로 관련 ====================
    
    def get_path(self, stage: str, dataset: str = 'maude') -> Path:
        """데이터 경로 반환
        
        Args:
            stage: 'bronze', 'silver', 'gold'
            dataset: 'maude', 'udi'
            
        Returns:
            Path 객체
        """
        use_s3 = self._base['paths']['use_s3']
        
        if use_s3:
            base = self._storage['s3']['paths'][stage]
        else:
            base = self._base['paths']['local'][stage]
        
        filename = self._base['datasets'][dataset][f'{stage}_file']
        return Path(base) / filename
    
    # ==================== 디버그/개발 ====================
    
    def print_config(self, config_name: Optional[str] = None):
        """설정 출력 (디버깅용)
        
        Args:
            config_name: None이면 전체, 'cleaning' 등 특정 설정명
        """
        import json
        
        if config_name is None:
            configs = {
                'base': self._base,
                'columns': self._columns,
                'filtering': self._filtering,
                'cleaning': self._cleaning,
                'deduplication': self._deduplication,
                'udi_matching': self._udi_matching,
                'transformation': self._transformation,
            }
        else:
            configs = {config_name: getattr(self, f'_{config_name}')}
        
        print(json.dumps(configs, indent=2, ensure_ascii=False))


# 싱글톤 인스턴스 (선택적)
_config = None

def get_config() -> PreprocessConfig:
    """전역 설정 인스턴스 반환 (싱글톤)"""
    global _config
    if _config is None:
        _config = PreprocessConfig()
    return _config