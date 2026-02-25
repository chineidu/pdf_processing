from dataclasses import asdict
from typing import Any

from celery import Celery
from kombu import Queue

from src.config import app_config, app_settings


def create_celery_app() -> Celery:
    """Create and configure a Celery application instance."""
    celery = Celery("ner_tasks")

    # Define priority queues for RabbitMQ
    task_queues = (
        # Priority queues
        Queue(
            app_config.queue_config.high_priority_ml,
            routing_key=app_config.queue_config.high_priority_ml,
        ),
        Queue(
            app_config.queue_config.normal_priority_ml,
            routing_key=app_config.queue_config.normal_priority_ml,
        ),
        Queue(
            app_config.queue_config.low_priority_ml,
            routing_key=app_config.queue_config.low_priority_ml,
        ),
        # Default queues
        Queue("celery", routing_key="celery"),  # default celery queue
        Queue(
            app_config.queue_config.cleanups,
            routing_key=app_config.queue_config.cleanups,
        ),
        Queue(
            app_config.queue_config.notifications,
            routing_key=app_config.queue_config.notifications,
        ),
    )

    # Convert to dictionary
    beat_config_dict: dict[str, dict[str, Any]] = asdict(
        app_config.celery_config.beat_config
    )
    task_routes: dict[str, str] = asdict(app_config.celery_config).get(
        "task_routes", {}
    )

    # Configuration
    celery.conf.update(
        # DB result backend config
        result_backend=app_settings.celery_database_url,
        result_backend_always_retry=app_config.celery_config.other_config.result_backend_always_retry,
        result_persistent=app_config.celery_config.other_config.result_persistent,
        result_backend_max_retries=app_config.celery_config.other_config.result_backend_max_retries,
        result_expires=app_config.celery_config.other_config.result_expires,
        # Broker config
        broker_url=app_settings.rabbitmq_celery_url,
        # RabbitMQ transport options for priority queues
        broker_transport_options={
            "priority_steps": list(range(10)),
            "sep": ":",
            "queue_order_strategy": "priority",
        },
        # Priority queue definitions
        task_queues=task_queues,
        # Task config
        task_serializer=app_config.celery_config.task_config.task_serializer,
        result_serializer=app_config.celery_config.task_config.result_serializer,
        accept_content=app_config.celery_config.task_config.accept_content,
        timezone=app_config.celery_config.task_config.timezone,
        enable_utc=app_config.celery_config.task_config.enable_utc,
        task_compression=app_config.celery_config.other_config.task_compression,
        # Task routing
        task_routes=task_routes,
        # Beat schedule
        beat_schedule=beat_config_dict.get("beat_schedule", {}),  # dict is required!
        # Worker config
        worker_prefetch_multiplier=app_config.celery_config.worker_config.worker_prefetch_multiplier,
        worker_max_tasks_per_child=app_config.celery_config.worker_config.worker_max_tasks_per_child,
        worker_max_memory_per_child=app_config.celery_config.worker_config.worker_max_memory_per_child,
        task_acks_late=app_config.celery_config.worker_config.task_acks_late,
        result_compression=app_config.celery_config.other_config.result_compression,
    )
    # Task discovery
    celery.autodiscover_tasks(
        [
            # Paths to task modules for auto-discovery
            "src.celery_app.tasks.periodic",
            "src.celery_app.tasks.processor",
            "src.celery_app.tasks.notifications",
        ]
    )

    return celery


celery_app = create_celery_app()
