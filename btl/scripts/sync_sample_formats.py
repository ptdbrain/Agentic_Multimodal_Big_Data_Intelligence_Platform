"""Inspect legacy format drift or derive new fixtures without overwriting raw inputs."""
import argparse
import json
from pathlib import Path
import pandas as pd


def _frame(path):
    path=Path(path)
    if path.suffix == '.parquet': return pd.read_parquet(path)
    if path.suffix == '.csv': return pd.read_csv(path)
    return pd.DataFrame(json.loads(path.read_text()))


def compare_formats(paths,key):
    frames=[_frame(p).sort_values(key).reset_index(drop=True) for p in paths]
    reference=frames[0].reindex(sorted(frames[0].columns),axis=1)
    for frame in frames[1:]:
        frame=frame.reindex(sorted(frame.columns),axis=1)
        try:
            pd.testing.assert_frame_equal(reference,frame,check_dtype=False,check_exact=False)
        except AssertionError:
            return False
    return True


def sync_samples(source_dir,output_dir,apply=False):
    source_dir,output_dir=Path(source_dir),Path(output_dir)
    result={}
    for name,key in [('products','product_id'),('reviews','review_id'),('prices','price_id')]:
        source=source_dir/f'{name}.json'
        frame=_frame(source)
        result[name]={'rows':len(frame),'source':str(source),'output':str(output_dir),'written':False}
        if apply:
            output_dir.mkdir(parents=True,exist_ok=True)
            for suffix in ['json','csv','parquet']:
                if (output_dir/f'{name}.{suffix}').exists():
                    raise FileExistsError('Refusing to overwrite existing derived fixture')
            (output_dir/f'{name}.json').write_text(source.read_text())
            frame.to_csv(output_dir/f'{name}.csv',index=False)
            frame.to_parquet(output_dir/f'{name}.parquet',index=False)
            result[name]['written']=True
    return result


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--source-dir',default='data/sample')
    p.add_argument('--output-dir',default='data/fixtures/canonical')
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    print(json.dumps(sync_samples(args.source_dir,args.output_dir,args.apply),indent=2))
