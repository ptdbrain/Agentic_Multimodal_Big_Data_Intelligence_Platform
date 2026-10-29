"""Deterministic clean inputs for infrastructure acceptance; explicitly synthetic."""
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from data.generator import generate_products,generate_reviews,generate_price_history


def create(folder):
    folder=Path(folder);folder.mkdir(parents=True,exist_ok=True)
    products=list({p['product_id']:p for p in generate_products(seed=42)}.values())
    datasets={'products':products,'reviews':generate_reviews(products,count=20,dirty_ratio=0,duplicate_ratio=0,inject_anomalies=False,seed=42),
              'prices':generate_price_history(products,seed=42)}
    for name,records in datasets.items():
        for r in records:r['source']='synthetic_acceptance_fixture'
        (folder/f'{name}.json').write_text(json.dumps(records,ensure_ascii=False))
    return datasets


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('output_dir');create(p.parse_args().output_dir)
