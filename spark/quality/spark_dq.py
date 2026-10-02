from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class SparkDataQualityEvaluator:
    """Data Quality evaluation using Spark aggregations."""
    
    @staticmethod
    def evaluate(df: DataFrame, dataset_name: str) -> dict:
        """Returns: total_count, valid_count, invalid_count, null_count, 
        duplicate_count, dq_score - all computed via Spark."""
        total_count = df.count()
        if total_count == 0:
            return {
                'dataset_name': dataset_name,
                'total_count': 0,
                'valid_count': 0,
                'invalid_count': 0,
                'null_count': 0,
                'duplicate_count': 0,
                'dq_score': 0.0
            }
            
        null_counts = [F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]
        if null_counts:
            nulls_row = df.agg(*null_counts).collect()[0]
            total_nulls = sum(nulls_row[c] for c in df.columns if nulls_row[c] is not None)
        else:
            total_nulls = 0
            
        distinct_count = df.distinct().count()
        duplicate_count = total_count - distinct_count
        
        # Simplified definitions for evaluation
        invalid_count = total_nulls
        valid_count = total_count - invalid_count
        if valid_count < 0:
            valid_count = 0
        dq_score = valid_count / total_count if total_count > 0 else 0.0
        
        return {
            'dataset_name': dataset_name,
            'total_count': total_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'null_count': total_nulls,
            'duplicate_count': duplicate_count,
            'dq_score': dq_score
        }
    
    @staticmethod
    def generate_report(metrics: dict, batch_id: str) -> dict:
        """Generate DQ report for gold/data_quality/"""
        return {
            'batch_id': batch_id,
            'metrics': metrics,
            'status': 'PASSED' if metrics.get('dq_score', 0) > 0.9 else 'WARNING'
        }
