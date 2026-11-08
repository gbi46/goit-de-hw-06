import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import kafka_config

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

try:
    while True:
        data = {
            "id": random.randint(1, 1_000_000),
            "temperature": round(random.uniform(45, 55), 2),
            "humidity": round(random.uniform(15, 85), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send("building_sensors", data).get(timeout=10)
        print(data)
        time.sleep(2)
except KeyboardInterrupt:
    pass
finally:
    producer.close()
