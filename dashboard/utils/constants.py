# constants.py
"""공통 상수 및 설정 값 (YAML에서 로드)"""

import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
root_path = Path(__file__).parent.parent.parent
sys.path.append(str(root_path))

from dashboard.utils.dashboard_config import get_config


# Config 로드
_config = get_config()
_defaults_config = _config._defaults if hasattr(_config, '_defaults') else {}


class ColumnNames:
    """데이터베이스 컬럼명 상수"""
    _cols = _defaults_config.get('columns', {})

    MANUFACTURER = _cols.get('manufacturer', 'manufacturer_name')
    PRODUCT_CODE = _cols.get('product_code', 'product_code')
    DATE_RECEIVED = _cols.get('date_received', 'date_received')
    DATE_OCCURRED = _cols.get('date_occurred', 'date_occurred')
    DEFECT_TYPE = _cols.get('defect_type', 'defect_type')
    PROBLEM_COMPONENTS = _cols.get('problem_components', 'problem_components')
    EVENT_TYPE = _cols.get('event_type', 'event_type')
    PATIENT_HARM = _cols.get('patient_harm', 'patient_harm')
    DEFECT_CONFIRMED = _cols.get('defect_confirmed', 'defect_confirmed')
    UDI_DI = _cols.get('udi_di', 'udi_di')
    CLUSTER = _cols.get('cluster', 'cluster')


class EventTypes:
    """이벤트 타입 상수"""
    _event_types = _defaults_config.get('event_types', {})

    DEATH = _event_types.get('death', 'Death')
    INJURY = _event_types.get('injury', 'Injury')
    SERIOUS_INJURY = _event_types.get('serious_injury', 'Serious Injury')
    MALFUNCTION = _event_types.get('malfunction', 'Malfunction')


class PatientHarmLevels:
    """환자 피해 등급"""
    _harm_levels = _defaults_config.get('patient_harm_levels', {})

    SERIOUS = _harm_levels.get('serious', ['Serious Injury', 'Death'])
    MINOR = _harm_levels.get('minor', ['Minor Injury'])
    NONE = _harm_levels.get('none', ['No Apparent Injury'])


class Defaults:
    """기본 설정 값"""
    _defaults = _defaults_config.get('defaults', {})

    # 분석 기본값
    TOP_N = _defaults.get('top_n', 10)
    MIN_CASES = _defaults.get('min_cases', 10)
    WINDOW_SIZE = _defaults.get('window_size', 1)
    DATE_FORMAT = _defaults.get('date_format', "%Y-%m")

    # UI 설정
    CHART_HEIGHT = _defaults.get('chart_height', 600)
    MAX_ITEMS_DISPLAY = _defaults.get('max_items_display', 100)

    # 제외 값
    EXCLUDE_DEFECT_TYPES = _defaults.get('exclude_defect_types', ['Other', 'Unknown'])
    MISSING_VALUE_LABEL = _defaults.get('missing_value_label', '(정보 없음)')

    # CFR 기본값
    _cfr = _defaults.get('cfr_defaults', {})
    CFR_TOP_N = _cfr.get('top_n', 20)
    CFR_MIN_CASES = _cfr.get('min_cases', 10)

    # 부품 분석 기본값
    _component = _defaults.get('component_defaults', {})
    COMPONENT_TOP_N = _component.get('top_n', 10)


class ChartStyles:
    """차트 스타일 설정"""
    _styles = _defaults_config.get('chart_styles', {})

    # 색상
    _colors = _styles.get('colors', {})
    PRIMARY_COLOR = _colors.get('primary', '#1f77b4')
    DANGER_COLOR = _colors.get('danger', '#d62728')
    WARNING_COLOR = _colors.get('warning', '#ff7f0e')
    SUCCESS_COLOR = _colors.get('success', '#2ca02c')

    # Plotly 설정
    PLOTLY_CONFIG = _styles.get('plotly', {
        'margin': {'l': 50, 'r': 20, 't': 40, 'b': 80},
        'hovermode': 'x unified'
    })
