from typing import Dict, Type, List, Any
from ingestion.crawler.base import BaseEcommerceCrawler
from ingestion.crawler.tiki import TikiCrawler
from ingestion.crawler.generic_html import UniversalProductScraper
from ingestion.crawler.mock import MockEcommerceCrawler

class CrawlerRegistry:
    """Dynamic Plugin Registry for E-Commerce Crawlers.
    Enables zero-code extension for new marketplaces (Shopee, Lazada, Amazon, etc.).
    """
    _registry: Dict[str, Type[BaseEcommerceCrawler]] = {}

    @classmethod
    def register(cls, platform_name: str, crawler_cls: Type[BaseEcommerceCrawler]):
        key = platform_name.lower().strip()
        cls._registry[key] = crawler_cls

    @classmethod
    def get(cls, platform_name: str, **kwargs) -> BaseEcommerceCrawler:
        key = platform_name.lower().strip()
        if key not in cls._registry:
            available = ", ".join(cls._registry.keys())
            raise ValueError(f"Unknown crawler platform: '{platform_name}'. Available: [{available}]")
        crawler_cls = cls._registry[key]
        return crawler_cls(**kwargs)

    @classmethod
    def list_available(cls) -> List[str]:
        return sorted(list(cls._registry.keys()))

# Pre-register built-in crawlers
CrawlerRegistry.register("tiki", TikiCrawler)
CrawlerRegistry.register("universal", UniversalProductScraper)
CrawlerRegistry.register("mock", MockEcommerceCrawler)
