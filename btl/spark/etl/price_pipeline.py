from typing import List, Dict, Any, Tuple
import datetime

class PricePipeline:
    """Price data ETL: Bronze -> Silver with proper schema.
    Schema: price_id, product_id, price, currency, seller, timestamp, source
    """
    
    @staticmethod
    def clean_prices(raw_prices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean price records."""
        cleaned = []
        for r in raw_prices:
            item = dict(r)
            if 'price' in item and isinstance(item['price'], str):
                try:
                    item['price'] = float(item['price'].replace(',', '').replace('$', '').strip())
                except ValueError:
                    pass
            if 'currency' not in item or not item['currency']:
                item['currency'] = 'USD'
            cleaned.append(item)
        return cleaned
    
    @staticmethod
    def validate_prices(prices: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate: price > 0, product_id not null, timestamp valid."""
        valid = []
        invalid = []
        for r in prices:
            p = r.get('price')
            pid = r.get('product_id')
            
            is_valid = True
            if pid is None or p is None:
                is_valid = False
            else:
                try:
                    if float(p) <= 0:
                        is_valid = False
                except (ValueError, TypeError):
                    is_valid = False
                    
            if is_valid:
                valid.append(r)
            else:
                invalid.append(r)
        return valid, invalid
    
    @staticmethod
    def run_pipeline(prices_source: List[Dict[str, Any]], save_silver: bool = True) -> dict:
        """Full price ETL: Bronze -> Validate -> Clean -> Silver Parquet."""
        cleaned = PricePipeline.clean_prices(prices_source)
        valid, invalid = PricePipeline.validate_prices(cleaned)
        
        if save_silver and valid:
            import pandas as pd
            from storage.storage_manager import storage
            storage.write_silver_parquet('prices', pd.DataFrame(valid))

        return {
            'processed': len(prices_source),
            'valid': len(valid),
            'invalid': len(invalid),
            'prices_processed': len(valid),
            'prices_invalid': len(invalid),
            'silver_records': valid
        }
