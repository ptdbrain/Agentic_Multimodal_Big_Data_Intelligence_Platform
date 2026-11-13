"""Export explicit serving bindings from an already verified managed-source configuration."""
import argparse,os,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))


def main():
    import yaml
    from serving.registry import DatasetRegistry
    from orchestration.source_jobs import validate_config
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-jobs',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();config=validate_config(yaml.safe_load(args.source_jobs.read_text()))
    value={'version':1,'datasets':[{'id':job['id'],'label':job.get('label',job['id']),'kind':'web' if job['adapter']=='web' else 'commerce','source_group':job['source_group'],'postgres_schema':job['target']['postgres_schema'],'bucket':job['target']['bucket'],'environment_file':job['environment_file']} for job in config['jobs']]}
    descriptor=os.open(args.output,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
    try:
        with os.fdopen(descriptor,'w') as stream:stream.write(yaml.safe_dump(value,allow_unicode=True,sort_keys=False));stream.flush();os.fsync(stream.fileno())
        DatasetRegistry.load(args.output)
    except BaseException:
        args.output.unlink(missing_ok=True);raise
    print('Created validated dataset registry; secret values are not included')


if __name__=='__main__':main()
