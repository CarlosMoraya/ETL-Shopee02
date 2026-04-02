"""
Extractor: Visão Geral de Motoristas - Total Expedido
Fonte: Shopee Logistics - Delivery Dashboard
Destino: data/raw/shopee_monitoramento_raw.csv
"""
import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime

from src.utils import (
    get_logger,
    get_env,
    DATA_RAW_DIR,
    SHOPEE_EMAIL,
    SHOPEE_PWD,
)

logger = get_logger(__name__)


async def login_shopee(page, email: str, password: str):
    """
    Realiza login no portal da Shopee Logistics.
    
    Args:
        page: Página do Playwright
        email: Email de login
        password: Senha de login
    """
    logger.info("Acessando portal da Shopee Logistics...")
    portal_url = "https://logistics.myagencyservice.com.br/"
    await page.goto(portal_url, wait_until="networkidle")
    
    logger.info("Preenchendo credenciais...")
    await page.locator('input[autocomplete="email"]').fill(email)
    await page.locator('input[type="password"]').fill(password)
    
    logger.info("Submetendo login...")
    await page.locator('input[type="password"]').press("Enter")
    
    # Aguardar confirmação de login
    try:
        await page.locator('text="Força de trabalho"').wait_for(timeout=30000)
        logger.info("Login realizado com sucesso!")
    except Exception:
        await page.screenshot(path="erro_login.png")
        logger.error("Falha no login. Verifique as credenciais.")
        raise Exception("Falha no login. Consulte erro_login.png")


async def navegar_para_monitoramento(page):
    """
    Navega até a tela de Monitoramento de Motoristas.
    
    Args:
        page: Página do Playwright
    """
    logger.info("Navegando para Delivery Dashboard...")
    dashboard_url = "https://logistics.myagencyservice.com.br/#/mgmtAgency/lm-hub"
    await page.goto(dashboard_url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(5000)
    
    # Clicar em "Visão geral dos motoristas"
    logger.info("Selecionando aba 'Visão geral dos motoristas'...")
    try:
        aba_motoristas = page.locator('span:has-text("Visão geral dos motoristas")')
        await aba_motoristas.wait_for(timeout=20000)
        await aba_motoristas.click()
        logger.info("Aba selecionada!")
    except Exception as e:
        logger.warning(f"Aba já pode estar ativa: {e}")
    
    await page.wait_for_timeout(5000)
    
    # Garantir filtro "Total Expedido"
    logger.info("Verificando filtro 'Total Expedido'...")
    try:
        filtro = page.locator('button:has-text("Total Expedido"), div:has-text("Total Expedido")')
        await filtro.wait_for(timeout=5000)
        is_active = await filtro.get_attribute('class')
        if 'active' not in str(is_active).lower() and 'selected' not in str(is_active).lower():
            logger.info("Clicando em 'Total Expedido'...")
            await filtro.click()
            await page.wait_for_timeout(2000)
        else:
            logger.info("Filtro 'Total Expedido' já ativo.")
    except Exception as e:
        logger.warning(f"Filtro não encontrado ou já ativo: {e}")


async def baixar_csv(page, output_path: Path) -> Path:
    """
    Baixa o CSV de monitoramento de motoristas.
    
    Args:
        page: Página do Playwright
        output_path: Caminho para salvar o arquivo
        
    Returns:
        Path: Caminho do arquivo baixado
    """
    logger.info("Procurando botão de Exportar...")
    export_button = page.locator('button:has-text("Exportar")')
    await export_button.wait_for(timeout=10000)
    
    logger.info("Iniciando download...")
    async with page.expect_download(timeout=30000) as download_info:
        await export_button.click()
    
    download = await download_info.value
    
    # Salvar na pasta data/raw
    caminho_arquivo = output_path / download.suggested_filename
    await download.save_as(caminho_arquivo)
    logger.info(f"Arquivo baixado: {caminho_arquivo}")
    
    return caminho_arquivo


def transformar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma os dados extraindo ID do motorista do nome.
    
    Args:
        df: DataFrame original
        
    Returns:
        DataFrame: DataFrame transformado
    """
    logger.info("Transformando dados...")
    
    # Separar ID do nome (formato: [ID] Nome)
    if 'Driver Name' in df.columns:
        extracao = df['Driver Name'].str.extract(r'\[(.*?)\]\s*(.*)')
        df.insert(0, 'driver_id', extracao[0])
        df['Driver Name'] = extracao[1]
        
        # Tratar nulos
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
        logger.info("Corrigido: expected_delivered_percentage_perc -> expected_delivered_percentage")
    
    # Log de totais
    logger.info("\n=== TOTAIS EXTRAÍDOS ===")
    if 'assigned' in df.columns:
        logger.info(f"Total Assigned: {df['assigned'].sum()}")
    if 'handed_over' in df.columns:
        logger.info(f"Total Handed Over: {df['handed_over'].sum()}")
    if 'delivered_qtd' in df.columns:
        logger.info(f"Total Delivered: {df['delivered_qtd'].sum()}")
    if 'on_hold' in df.columns:
        logger.info(f"Total On-Hold: {df['on_hold'].sum()}")
    if 'delivering_qtd' in df.columns:
        logger.info(f"Total Delivering: {df['delivering_qtd'].sum()}")
    logger.info(f"Total Motoristas: {len(df)}")
    logger.info("========================\n")
    
    return df


async def extract_shopee_monitoramento() -> Path:
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
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_file = output_path / f"raw_{timestamp}.xlsx"
    processed_file = output_path / f"processed_{timestamp}.csv"
    
    headless = os.environ.get("CRAWLER_HEADLESS", "true").lower() == "true"
    
    async with async_playwright() as p:
        logger.info(f"Iniciando navegador (Headless: {headless})...")
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            locale="pt-BR",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        
        try:
            # Login
            await login_shopee(page, SHOPEE_EMAIL, SHOPEE_PWD)
            
            # Navegar até a tela
            await navegar_para_monitoramento(page)
            
            # Baixar arquivo
            arquivo_baixado = await baixar_csv(page, output_path)
            
            # Ler e transformar dados
            logger.info("Lendo arquivo Excel...")
            df = pd.read_excel(arquivo_baixado)
            
            # Transformar
            df_transformado = transformar_dados(df)
            
            # Salvar processado
            df_transformado.to_csv(processed_file, index=False)
            logger.info(f"Dados processados salvos em: {processed_file}")
            
            return processed_file
            
        except Exception as e:
            logger.error(f"[FALHA] Motivo: {e}")
            await page.screenshot(path="erro_crawler.png")
            logger.error("Consulte 'erro_crawler.png'")
            raise
        finally:
            logger.info("Fechando navegador...")
            await browser.close()


async def run():
    """
    Função principal para executar o crawler.
    """
    try:
        arquivo_processado = await extract_shopee_monitoramento()
        logger.info(f"✅ Extração concluída: {arquivo_processado}")
        return str(arquivo_processado)
    except Exception as e:
        logger.error(f"❌ Falha na extração: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run())
