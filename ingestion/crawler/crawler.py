from typing import Dict, Any, List, Optional
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.crawler.registry import CrawlerRegistry
from ingestion.crawler.mock import MockEcommerceCrawler
from ingestion.crawler.tiki import TikiCrawler, PoliteTikiCrawler
from ingestion.crawler.generic_html import UniversalProductScraper
from ingestion.crawler.validator import CrawlerDataValidator, validate_crawler_payloads

class WebCrawler(MockEcommerceCrawler):
    """Backward-compatible WebCrawler maintaining legacy API while integrating
    with the new extensible crawler ecosystem.
    """
    pass

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
