# config/config_loader.py (범용 로더 - 저수준)
import yaml
import os
from pathlib import Path
from functools import lru_cache
from typing import Dict, Any

class ConfigLoader:
    """범용 YAML 설정 로더 (싱글톤)"""
    
    def __init__(self) -> None:
        self.project_root = self._find_project_root()
        self.config_dir = self.project_root / 'config'
    
    def _find_project_root(self) -> Path:
        """프로젝트 루트 자동 탐색"""
        current = Path(__file__).resolve()
        for parent in current.parents:
            if (parent / 'requirements.txt').exists():
                return parent
        return current.parent.parent
    
    @lru_cache(maxsize=32)
    def load(self, config_name: str) -> Dict[Any, Any]:
        """YAML 파일 로드 및 캐싱
        
        Args:
            config_name: 'base', 'preprocess/cleaning' 등
            
        Returns:
            파싱된 설정 딕셔너리
        """
        config_path = self.config_dir / f'{config_name}.yaml'
        
        if not config_path.exists():
            raise FileNotFoundError(f'Config not found: {config_path}')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        return self._replace_env_vars(config)
    
    def _replace_env_vars(self, config: Any) -> Any:
        """환경변수 치환 (재귀적)
        
        ${VAR_NAME} 형태를 실제 환경변수 값으로 치환
        """
        if isinstance(config, dict):
            return {k: self._replace_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._replace_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            value = os.getenv(env_var)
            if value is None:
                raise ValueError(f"Environment variable not found: {env_var}")
            return value
        return config

# 싱글톤 인스턴스
_loader = ConfigLoader()

def load_config(config_name: str) -> Dict[Any, Any]:
    """함수형 인터페이스"""
    return _loader.load(config_name)