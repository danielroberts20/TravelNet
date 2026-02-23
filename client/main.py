import cleaning
import enrichment
import feature_engineering
from client.logging_config import configure_logging
from server_interaction import download_database

def start_client():
    configure_logging()
    download_database()

cleaned_data = cleaning.dummy_func(None)
enriched_data = enrichment.dummy_func(cleaned_data)
c = feature_engineering.dummy_func(enriched_data)

if __name__ == "__main__":
    start_client()