"""
kafka_setup.py
Creates and configures NHL Kafka topics with 24-hour retention.
Run once before starting the producer.
"""

import logging
import sys
from pathlib import Path

import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("kafka_setup")


def load_config() -> dict:
    import os, re
    config_path = Path(__file__).parents[2] / "config" / "config.yaml"
    with open(config_path) as f:
        raw = f.read()
    raw = re.sub(r"\$\{(\w+)\}", lambda m: os.getenv(m.group(1), m.group(0)), raw)
    return yaml.safe_load(raw)


def build_admin_client(cfg: dict) -> AdminClient:
    kafka_cfg = cfg["kafka"]
    conf = {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "socket.connection.setup.timeout.ms": 10000,
    }
    # Add SASL/SSL when configured for cloud brokers
    if kafka_cfg.get("security_protocol") != "PLAINTEXT":
        import os
        conf.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
                "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
            }
        )
    return AdminClient(conf)


def create_topics(admin: AdminClient, cfg: dict) -> None:
    kafka_cfg = cfg["kafka"]
    topics_to_create = []

    for logical_name, topic_name in kafka_cfg["topics"].items():
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=kafka_cfg["partitions"],
            replication_factor=kafka_cfg["replication_factor"],
            config={
                "retention.ms": str(kafka_cfg["retention_ms"]),
                "cleanup.policy": "delete",
                "compression.type": "snappy",
                # Keeps at least one message per key so consumers can replay
                "min.insync.replicas": "1",
            },
        )
        topics_to_create.append(new_topic)
        log.info(
            "Queued topic '%s' (%s) — %d partition(s), retention %d ms",
            topic_name,
            logical_name,
            kafka_cfg["partitions"],
            kafka_cfg["retention_ms"],
        )

    futures = admin.create_topics(topics_to_create, validate_only=False)

    all_ok = True
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info("Topic '%s' created successfully.", topic_name)
        except KafkaException as exc:
            err_code = exc.args[0].code()
            # TOPIC_ALREADY_EXISTS is not fatal — idempotent re-runs
            if err_code == KafkaError.TOPIC_ALREADY_EXISTS:
                log.warning("Topic '%s' already exists — skipping.", topic_name)
            else:
                log.error("Failed to create topic '%s': %s", topic_name, exc)
                all_ok = False

    if not all_ok:
        sys.exit(1)


def update_topic_retention(admin: AdminClient, cfg: dict) -> None:
    """Idempotently update retention on existing topics."""
    from confluent_kafka.admin import ConfigResource, ConfigEntry, AlterConfigOpType

    kafka_cfg = cfg["kafka"]
    resources = []
    for topic_name in kafka_cfg["topics"].values():
        resource = ConfigResource("topic", topic_name)
        resource.set_config("retention.ms", str(kafka_cfg["retention_ms"]))
        resources.append(resource)

    futures = admin.incremental_alter_configs(resources)
    for resource, future in futures.items():
        try:
            future.result()
            log.info("Retention updated for '%s'.", resource.name)
        except KafkaException as exc:
            log.error("Could not update retention for '%s': %s", resource.name, exc)


def main() -> None:
    cfg = load_config()
    log.info(
        "Connecting to Kafka at %s ...", cfg["kafka"]["bootstrap_servers"]
    )
    admin = build_admin_client(cfg)
    create_topics(admin, cfg)
    log.info("Kafka topic setup complete.")


if __name__ == "__main__":
    main()
