import pkgutil

__path__ = pkgutil.extend_path(__path__, __name__)

try:
    from kafka.producer import KafkaProducer
    from kafka.consumer import KafkaConsumer
    from kafka.admin import KafkaAdminClient
except ImportError:
    KafkaProducer = None
    KafkaConsumer = None
    KafkaAdminClient = None
