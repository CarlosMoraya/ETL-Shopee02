"""
Utilitários do projeto ETL Shopee
"""
from .logger import get_logger
from .config import (
    get_env,
    get_neon_connection_string,
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    SHOPEE_EMAIL,
    SHOPEE_PWD,
)
from .browser_manager import create_browser, create_context, create_page

__all__ = [
    "get_logger",
    "get_env",
    "get_neon_connection_string",
    "DATA_RAW_DIR",
    "DATA_PROCESSED_DIR",
    "SHOPEE_EMAIL",
    "SHOPEE_PWD",
    "create_browser",
    "create_context",
    "create_page",
]
