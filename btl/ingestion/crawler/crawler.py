from typing import Dict, Any, List, Optional
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.crawler.registry import CrawlerRegistry
from ingestion.crawler.mock import MockEcommerceCrawler
from ingestion.crawler.tiki import TikiCrawler, PoliteTikiCrawler
from ingestion.crawler.generic_html import UniversalProductScraper
from ingestion.crawler.validator import CrawlerDataValidator, validate_crawler_payloads

class WebCrawler(MockEcommerceCrawler):
    """Legacy simulation shim; use an explicit real adapter for network collection."""

    def __init__(self, *args, simulation=False, **kwargs):
        if simulation is not True:
            raise ValueError('WebCrawler is a legacy simulation; opt in with simulation=True or use a real adapter')
        super().__init__(*args, **kwargs)

__all__ = [
    "WebCrawler",
    "BaseEcommerceCrawler",
    "CrawlerRegistry",
    "TikiCrawler",
    "PoliteTikiCrawler",
    "UniversalProductScraper",
    "MockEcommerceCrawler",
    "CrawlerDataValidator",
    "validate_crawler_payloads",
]
