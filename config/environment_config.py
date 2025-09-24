"""
Environment-specific configuration settings.

This module contains configuration settings for different environments
(development, staging, production) and deployment scenarios.
"""

import os
from pathlib import Path
from typing import Dict, Any


class EnvironmentConfig:
    """Base configuration class for different environments."""
    
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self._load_config()
    
    def _load_config(self):
        """Load configuration based on environment."""
        if self.environment == "development":
            self._load_development_config()
        elif self.environment == "staging":
            self._load_staging_config()
        elif self.environment == "production":
            self._load_production_config()
        else:
            raise ValueError(f"Unknown environment: {self.environment}")
    
    def _load_development_config(self):
        """Development environment configuration."""
        self.DATA_PATH = os.getenv("DATA_PATH", "/tmp/etl_data")
        self.OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/etl_output")
        self.LOG_LEVEL = "DEBUG"
        self.ENABLE_DASK = False
        self.DASK_WORKERS = 1
        self.DASK_MEMORY_LIMIT = "2GB"
        self.PREFECT_SERVER_URL = "http://localhost:4200"
        self.ENABLE_MONITORING = False
        
    def _load_staging_config(self):
        """Staging environment configuration."""
        self.DATA_PATH = os.getenv("DATA_PATH", "/data/staging")
        self.OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/output/staging")
        self.LOG_LEVEL = "INFO"
        self.ENABLE_DASK = True
        self.DASK_WORKERS = 2
        self.DASK_MEMORY_LIMIT = "4GB"
        self.PREFECT_SERVER_URL = os.getenv("PREFECT_SERVER_URL", "http://prefect-staging:4200")
        self.ENABLE_MONITORING = True
        
    def _load_production_config(self):
        """Production environment configuration."""
        self.DATA_PATH = os.getenv("DATA_PATH", "/data/production")
        self.OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/output/production")
        self.LOG_LEVEL = "WARNING"
        self.ENABLE_DASK = True
        self.DASK_WORKERS = 4
        self.DASK_MEMORY_LIMIT = "8GB"
        self.PREFECT_SERVER_URL = os.getenv("PREFECT_SERVER_URL", "http://prefect-prod:4200")
        self.ENABLE_MONITORING = True
        
    def get_config(self) -> Dict[str, Any]:
        """Get configuration dictionary."""
        return {
            "environment": self.environment,
            "data_path": self.DATA_PATH,
            "output_path": self.OUTPUT_PATH,
            "log_level": self.LOG_LEVEL,
            "enable_dask": self.ENABLE_DASK,
            "dask_workers": self.DASK_WORKERS,
            "dask_memory_limit": self.DASK_MEMORY_LIMIT,
            "prefect_server_url": self.PREFECT_SERVER_URL,
            "enable_monitoring": self.ENABLE_MONITORING
        }


def get_environment_config(environment: str = None) -> EnvironmentConfig:
    """
    Get configuration for specified environment.
    
    Args:
        environment: Environment name (development, staging, production)
        
    Returns:
        EnvironmentConfig instance
    """
    if environment is None:
        environment = os.getenv("ENVIRONMENT", "development")
    
    return EnvironmentConfig(environment)


# Environment-specific settings
DEVELOPMENT_CONFIG = {
    "data_validation": {
        "strict_mode": False,
        "allow_missing_files": True,
        "sample_size": 1000
    },
    "processing": {
        "chunk_size": 10000,
        "parallel_processing": False,
        "cache_intermediate_results": True
    },
    "logging": {
        "level": "DEBUG",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "file_logging": True
    }
}

STAGING_CONFIG = {
    "data_validation": {
        "strict_mode": True,
        "allow_missing_files": False,
        "sample_size": None
    },
    "processing": {
        "chunk_size": 50000,
        "parallel_processing": True,
        "cache_intermediate_results": False
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "file_logging": True
    }
}

PRODUCTION_CONFIG = {
    "data_validation": {
        "strict_mode": True,
        "allow_missing_files": False,
        "sample_size": None
    },
    "processing": {
        "chunk_size": 100000,
        "parallel_processing": True,
        "cache_intermediate_results": False
    },
    "logging": {
        "level": "WARNING",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "file_logging": True
    }
}

# Cloud deployment configurations
CLOUD_CONFIGS = {
    "aws": {
        "data_path": "s3://your-bucket/data/",
        "output_path": "s3://your-bucket/output/",
        "dask_cluster": "dask-kubernetes",
        "prefect_server": "prefect-cloud"
    },
    "azure": {
        "data_path": "abfss://your-container@your-storage.dfs.core.windows.net/data/",
        "output_path": "abfss://your-container@your-storage.dfs.core.windows.net/output/",
        "dask_cluster": "dask-kubernetes",
        "prefect_server": "prefect-cloud"
    },
    "gcp": {
        "data_path": "gs://your-bucket/data/",
        "output_path": "gs://your-bucket/output/",
        "dask_cluster": "dask-kubernetes",
        "prefect_server": "prefect-cloud"
    }
}


def get_cloud_config(cloud_provider: str) -> Dict[str, Any]:
    """
    Get cloud-specific configuration.
    
    Args:
        cloud_provider: Cloud provider (aws, azure, gcp)
        
    Returns:
        Cloud configuration dictionary
    """
    if cloud_provider not in CLOUD_CONFIGS:
        raise ValueError(f"Unknown cloud provider: {cloud_provider}")
    
    return CLOUD_CONFIGS[cloud_provider]


def setup_environment(environment: str = None, cloud_provider: str = None):
    """
    Setup environment configuration.
    
    Args:
        environment: Environment name
        cloud_provider: Cloud provider for deployment
        
    Returns:
        Configuration dictionary
    """
    config = get_environment_config(environment)
    base_config = config.get_config()
    
    if cloud_provider:
        cloud_config = get_cloud_config(cloud_provider)
        base_config.update(cloud_config)
    
    # Set environment variables
    os.environ["DATA_PATH"] = base_config["data_path"]
    os.environ["OUTPUT_PATH"] = base_config["output_path"]
    
    return base_config


if __name__ == "__main__":
    # Example usage
    config = setup_environment("production", "aws")
    print(f"Configuration: {config}")
