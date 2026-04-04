"""
Logger padronizado para o projeto ETL
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
import zoneinfo


class BrasiliaFormatter(logging.Formatter):
    TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=self.TZ)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S")


def get_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Cria um logger configurado.
    
    Args:
        name: Nome do logger (geralmente __name__)
        log_file: Arquivo de log opcional
        
    Returns:
        logging.Logger: Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Evitar duplicação de handlers
    if logger.handlers:
        return logger
    
    # Formatter com fuso horário de Brasília
    formatter = BrasiliaFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para arquivo (opcional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(BrasiliaFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(file_handler)
    
    return logger
