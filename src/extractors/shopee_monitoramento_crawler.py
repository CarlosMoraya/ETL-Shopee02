"""
Extractor: Visão Geral de Motoristas - Total Expedido
Fonte: Shopee Logistics - API Direta
Destino: data/raw/shopee_monitoramento_raw.csv

ESTE CRAWLER NÃO USA BROWSER - Apenas requests HTTP
"""
import requests
import os
from pathlib import Path
from datetime import datetime
import time

from src.utils import get_logger, DATA_RAW_DIR, SHOPEE_EMAIL, SHOPEE_PWD

logger = get_logger(__name__)


class ShopeeExtractor:
    """
    Extrator da Shopee usando API direta (sem browser)
    """
    
    BASE_URL = "https://logistics.myagencyservice.com.br"
    LOGIN_URL = f"{BASE_URL}/mgmt/api/pc/login"
    EXPORT_URL = f"{BASE_URL}/mgmt/api/pc/agency/metric/lm/export_fleet_list_v2"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Origin": self.BASE_URL,
            "Referer": f"{self.BASE_URL}/",
        })
    
    def login(self, email: str, password: str) -> bool:
        """
        Realiza login no sistema e salva o cookie/token.
        
        Args:
            email: Email de login
            password: Senha
            
        Returns:
            bool: True se login sucesso
        """
        logger.info("Iniciando login...")
        
        try:
            # Payload de login (ajustar conforme necessário)
            payload = {
                "email": email,
                "password": password,
            }
            
            logger.info(f"Tentando login com email: {email}")
            
            response = self.session.post(
                self.LOGIN_URL,
                json=payload,
                timeout=30
            )
            
            logger.info(f"Status do login: {response.status_code}")
            
            if response.status_code == 200:
                dados = response.json()
                
                # Verificar se login foi bem sucedido
                if dados.get("success") or dados.get("token") or "JSESSIONID" in response.cookies:
                    logger.info("✅ Login realizado com sucesso!")
                    
                    # Salvar cookies para próximas requisições
                    logger.info(f"Cookies obtidos: {list(response.cookies.keys())}")
                    
                    return True
            
            # Tentar alternativa - talvez o login seja form-data
            logger.warning("Tentando login como form-data...")
            response = self.session.post(
                self.LOGIN_URL,
                data=payload,
                timeout=30
            )
            
            if response.status_code == 200 and "JSESSIONID" in response.cookies:
                logger.info("✅ Login realizado com form-data!")
                return True
            
            logger.error(f"Falha no login. Response: {response.text[:200]}")
            return False
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede no login: {e}")
            return False
    
    def baixar_export(self, output_path: Path) -> Path:
        """
        Baixa o arquivo de exportação da API.
        
        Args:
            output_path: Pasta para salvar o arquivo
            
        Returns:
            Path: Caminho do arquivo baixado
        """
        logger.info("Iniciando download da API...")
        
        try:
            # Parâmetros da requisição (ajustar conforme necessário)
            params = {
                "type": "total_expedido",  # Filtro: Total Expedido
                "timestamp": int(time.time() * 1000),  # Para evitar cache
            }
            
            logger.info(f"URL: {self.EXPORT_URL}")
            logger.info(f"Parâmetros: {params}")
            
            response = self.session.get(
                self.EXPORT_URL,
                params=params,
                timeout=60
            )
            
            logger.info(f"Status do download: {response.status_code}")
            
            if response.status_code == 200:
                # Determinar nome do arquivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Verificar Content-Disposition para nome do arquivo
                content_disp = response.headers.get("Content-Disposition", "")
                if "filename=" in content_disp:
                    filename = content_disp.split("filename=")[1].strip('"\'')
                else:
                    filename = f"shopee_motoristas_{timestamp}.xlsx"
                
                caminho_arquivo = output_path / filename
                
                # Salvar arquivo
                with open(caminho_arquivo, "wb") as f:
                    f.write(response.content)
                
                logger.info(f"✅ Arquivo baixado: {caminho_arquivo}")
                logger.info(f"Tamanho: {len(response.content)} bytes")
                
                return caminho_arquivo
            else:
                logger.error(f"Erro no download: {response.status_code}")
                logger.error(f"Response: {response.text[:500]}")
                raise Exception(f"Falha no download: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede no download: {e}")
            raise
    
    def close(self):
        """Fecha a sessão"""
        self.session.close()


def extract_shopee_monitoramento() -> Path:
    """
    Extrai dados de monitoramento de motoristas da Shopee.
    
    Returns:
        Path: Caminho do arquivo processado
    """
    logger.info("=" * 60)
    logger.info("INICIANDO EXTRAÇÃO: Shopee Monitoramento de Motoristas")
    logger.info("=" * 60)
    
    output_path = DATA_RAW_DIR / "shopee_monitoramento"
    output_path.mkdir(parents=True, exist_ok=True)
    
    extractor = ShopeeExtractor()
    
    try:
        # Login
        if not extractor.login(SHOPEE_EMAIL, SHOPEE_PWD):
            raise Exception("Falha no login. Verifique email e senha.")
        
        # Aguardar um pouco após login
        time.sleep(2)
        
        # Baixar arquivo
        arquivo_baixado = extractor.baixar_export(output_path)
        
        # Ler e transformar dados
        logger.info("Lendo arquivo Excel...")
        import pandas as pd
        df = pd.read_excel(arquivo_baixado)
        
        # Transformar (separar ID do nome)
        logger.info("Transformando dados...")
        if 'Driver Name' in df.columns:
            extracao = df['Driver Name'].str.extract(r'\[(.*?)\]\s*(.*)')
            df.insert(0, 'driver_id', extracao[0])
            df['Driver Name'] = extracao[1]
            df['driver_id'] = df['driver_id'].fillna('')
            df['Driver Name'] = df['Driver Name'].fillna('')
        
        # Normalizar nomes das colunas
        df.columns = (
            df.columns
            .str.replace('（', '(').str.replace('）', ')')
            .str.strip()
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('(#)', '_qtd', regex=False)
            .str.replace('(%)', '_perc', regex=False)
            .str.replace('(', '').str.replace(')', '')
            .str.replace('-', '_')
            .str.replace('__', '_')
            .str.strip('_')
        )
        
        # Corrigir nome problemático
        if 'expected_delivered_percentage_perc' in df.columns:
            df = df.rename(columns={'expected_delivered_percentage_perc': 'expected_delivered_percentage'})
        
        # Adicionar timestamp
        df['extracted_at'] = datetime.now()
        
        # Salvar processado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        processed_file = output_path / f"processed_{timestamp}.csv"
        df.to_csv(processed_file, index=False)
        logger.info(f"Dados processados salvos em: {processed_file}")
        
        # Log de totais
        logger.info("\n=== TOTAIS EXTRAÍDOS ===")
        if 'assigned' in df.columns:
            logger.info(f"Total Assigned: {df['assigned'].sum()}")
        if 'handed_over' in df.columns:
            logger.info(f"Total Handed Over: {df['handed_over'].sum()}")
        if 'delivered_qtd' in df.columns:
            logger.info(f"Total Delivered: {df['delivered_qtd'].sum()}")
        logger.info(f"Total Motoristas: {len(df)}")
        logger.info("========================\n")
        
        return processed_file
        
    finally:
        extractor.close()


async def run():
    """
    Função principal para executar o crawler.
    """
    try:
        arquivo_processado = extract_shopee_monitoramento()
        logger.info(f"✅ Extração concluída: {arquivo_processado}")
        return str(arquivo_processado)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
