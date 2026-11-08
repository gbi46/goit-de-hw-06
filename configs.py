import os

from dotenv import load_dotenv

load_dotenv()

kafka_config = {
    "bootstrap_servers": [os.getenv("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")],
    "username": os.getenv("KAFKA_USERNAME"),
    "password": os.getenv("KAFKA_PASSWORD"),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
}
