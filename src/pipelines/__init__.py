"""
Pipelines ETL Shopee
"""
from .shopee_monitoramento_pipeline import run_pipeline as run_monitoramento_pipeline

__all__ = [
    "run_monitoramento_pipeline",
]
