"""
설정 및 상수
"""
from dataclasses import dataclass

@dataclass
class Config:
    """UDI 처리 설정"""
    
    # 퍼지 매칭 임계값
    FUZZY_THRESHOLD: int = 90
    
    # 저준수 제조사 기준
    LOW_COMPLIANCE_THRESHOLD: float = 0.50
    
    # 신뢰도 매핑
    CONFIDENCE_MAP = {
        'original': 'HIGH',
        'extracted': 'HIGH',
        'single_match': 'HIGH',
        'time_inferred': 'MEDIUM',
        'freq_inferred': 'LOW',
        'fallback_oldest': 'LOW',
        'tier3': 'VERY_LOW'
    }
    
    # MAUDE 날짜 우선순위
    MAUDE_DATES = [
        'date_of_event',
        'date_received',
        'date_report',
        'device_0_date_received'
    ]
    
    # UDI DB 날짜
    UDI_DATES = ['publish_date', 'public_version_date']