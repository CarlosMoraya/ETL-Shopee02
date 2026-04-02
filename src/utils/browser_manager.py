"""
Gerenciador de navegador Playwright
Configurações compartilhadas para todos os crawlers
"""
import os
from playwright.async_api import async_playwright, Browser, BrowserContext, Page


async def create_browser(headless: bool = None):
    """
    Cria uma instância do navegador Chromium com configurações padronizadas.
    
    Args:
        headless: Se True, roda sem interface. Default: lê do env CRAWLER_HEADLESS
        
    Returns:
        Browser: Instância do navegador
    """
    if headless is None:
        headless = os.environ.get("CRAWLER_HEADLESS", "true").lower() == "true"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-accelerated-2d-canvas",
                "--disable-gpu",
            ]
        )
        return browser


async def create_context(browser: Browser) -> BrowserContext:
    """
    Cria um contexto de navegador com configurações padronizadas.
    
    Args:
        browser: Instância do navegador
        
    Returns:
        BrowserContext: Contexto configurado
    """
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        locale="pt-BR",
        viewport={"width": 1920, "height": 1080},
    )
    return context


async def create_page(context: BrowserContext) -> Page:
    """
    Cria uma página no contexto.
    
    Args:
        context: Contexto do navegador
        
    Returns:
        Page: Página configurada
    """
    page = await context.new_page()
    return page
