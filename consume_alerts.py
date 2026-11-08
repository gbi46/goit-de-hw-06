import json

from kafka import KafkaConsumer

from configs import kafka_config

topic = "building_sensors_alerts"

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    auto_offset_reset="earliest",
    group_id=None,
    enable_auto_commit=False,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Reading alerts from {topic}. Press Ctrl+C to stop.", flush=True)

try:
    for record in consumer:
        print(
            f"\nTOPIC={record.topic} "
            f"PARTITION={record.partition} OFFSET={record.offset}",
            flush=True,
        )
        print(json.dumps(record.value, ensure_ascii=False, indent=2), flush=True)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
