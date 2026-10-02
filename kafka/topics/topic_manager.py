import sys
from pathlib import Path
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import settings

from kafka.admin import KafkaAdminClient, NewTopic

def setup_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=settings.kafka.bootstrap_servers, 
            client_id='sentinel-admin',
            request_timeout_ms=5000
        )
        
        topic_list = []
        
        # Define topics based on settings and requirements
        topics_configs = [
            (settings.kafka.topic_products, 2),
            (settings.kafka.topic_reviews, 3),
            (settings.kafka.topic_prices, 2),
            (settings.kafka.topic_events, 3),
            ("dead-letter", 1)
        ]
        
        existing_topics = admin_client.list_topics()
        
        for topic_name, partitions in topics_configs:
            if topic_name not in existing_topics:
                topic_list.append(NewTopic(name=topic_name, num_partitions=partitions, replication_factor=1))
                
        if topic_list:
            print(f"Creating topics: {[t.name for t in topic_list]}")
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topics created successfully.")
        else:
            print("All topics already exist.")
            
    except Exception as e:
        print(f"Failed to setup topics: {e}")

if __name__ == "__main__":
    setup_topics()
