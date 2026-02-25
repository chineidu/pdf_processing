import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator

import aio_pika

from src import create_logger
from src.config import app_settings
from src.schemas.services.ingestion_worker import QueueArguments

if TYPE_CHECKING:
    from src.config.config import AppConfig


logger = create_logger(name=__name__)
STORAGE_RMQ_URL: str = app_settings.rabbitmq_storage_url


class BaseRabbitMQ:
    """RabbitMQ base class with connection management."""

    def __init__(self, config: "AppConfig", url: str = STORAGE_RMQ_URL) -> None:
        """Initialize the producer with configuration.

        Parameters
        ----------
        config : AppConfig
            Application configuration containing RabbitMQ settings.
        url : str
            RabbitMQ connection URL.


        Returns
        -------
        None
        """
        self.config = config
        self.url = url

        self.connection: aio_pika.RobustConnection | None = None
        self.channel: aio_pika.Channel | None = None
        self._lock = asyncio.Lock()

    async def aconnect(self) -> None:
        """Establish connection to RabbitMQ."""
        # Fast path: already connected
        if await self._is_connected():
            return

        # If multiple coroutines try to connect simultaneously, ensure only one does the work
        async with self._lock:
            if await self._is_connected():
                return

            # Connect with retries (if not already connected)
            for attempt in range(self.config.rabbitmq_config.max_retries):
                logger.info(
                    f"[+] Connecting to RabbitMQ (attempt {attempt + 1}/"
                    f"{self.config.rabbitmq_config.max_retries})"
                )
                try:
                    self.connection = await aio_pika.connect_robust(  # type: ignore
                        url=self.url,
                        timeout=self.config.rabbitmq_config.connection_timeout,
                        heartbeat=self.config.rabbitmq_config.heartbeat,
                    )
                    # Create a channel
                    if self.connection is not None:
                        self.channel = await self.connection.channel()  # type: ignore

                    # Set prefetch count
                    if self.channel is not None:
                        await self.channel.set_qos(
                            prefetch_count=self.config.rabbitmq_config.prefetch_count
                        )

                    if await self._is_connected():
                        logger.info("[+] Successfully connected to RabbitMQ")
                    return

                except aio_pika.exceptions.AMQPException as e:
                    if attempt < self.config.rabbitmq_config.max_retries - 1:
                        # Exponential backoff before retrying
                        delay = self.config.rabbitmq_config.retry_delay * (2**attempt)
                        logger.warning(
                            f"[-] Connection attempt {attempt + 1} failed: {e}. Retrying in {delay}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        # All retries exhausted
                        logger.error(
                            "[-] Failed to connect to RabbitMQ after all retries"
                        )
                        raise

    async def adisconnect(self) -> None:
        """Close the connection to RabbitMQ."""
        if self.connection is not None and not self.connection.is_closed:
            try:
                await self.connection.close()
                logger.info("[+] Disconnected from RabbitMQ")

            except Exception as e:
                logger.error(f"[-] Error disconnecting: {e}")
            finally:
                self.connection = None
                self.channel = None

    async def _is_connected(self) -> bool:
        """Check if the producer is connected to RabbitMQ.

        Returns
        -------
        bool
            True if connected, False otherwise.
        """
        return self.connection is not None and not self.connection.is_closed

    async def aensure_queue(
        self,
        queue_name: str,
        arguments: QueueArguments,
        durable: bool = True,
    ) -> aio_pika.abc.AbstractQueue:
        """Ensure queue exists with specified settings. Idempotent operation.

        Parameters
        ----------
        queue_name : str
            The name of the queue to ensure.
        arguments : QueueArguments
            Additional arguments for queue declaration (e.g. DLQ settings), by default None.
        durable : bool, optional
            Whether the queue should persist after broker restart, by default True.

        Returns
        -------
        aio_pika.abc.AbstractQueue
            The declared queue.
        """
        if self.channel is None:
            logger.debug("Channel not established, connecting...")
            await self.aconnect()

        if self.channel is None:
            raise RuntimeError("[-] Failed to establish channel connection")

        try:
            queue = await self.channel.declare_queue(
                queue_name, arguments=arguments.model_dump(), durable=durable
            )
            logger.debug(f"Queue '{queue_name}' ensured")
            return queue

        except aio_pika.exceptions.AMQPException as e:
            logger.error(
                f"Failed to declare queue '{queue_name}' with arguments={arguments}: {e}"
            )
            raise

    async def aensure_dlq(
        self,
        dlq_name: str,
        dlx_name: str,
    ) -> aio_pika.abc.AbstractQueue:
        """Ensure DLQ and DLX exists with specified settings. Idempotent operation.

        Parameters
        ----------
        dlq_name : str
            The name of the dead-letter queue to ensure.
        dlx_name : str
            The name of the dead-letter exchange to ensure.

        Returns
        -------
        aio_pika.abc.AbstractQueue
            The declared queue.
        """
        if self.channel is None:
            logger.debug("Channel not established, connecting...")
            await self.aconnect()

        if self.channel is None:
            raise RuntimeError("[-] Failed to establish channel connection")

        try:
            # Dead Letter Exchange (Fanout for simplicity)
            dlx = await self.channel.declare_exchange(
                dlx_name, aio_pika.ExchangeType.FANOUT, durable=True
            )
            # Dead Letter Queue
            queue = await self.channel.declare_queue(dlq_name, durable=True)
            # Bind DLQ to DLX
            await queue.bind(dlx)
            logger.info(f"DLQ '{dlq_name}' and DLX '{dlx_name}' ensured")
            return queue

        except aio_pika.exceptions.AMQPException as e:
            logger.error(f"Failed to ensure DLQ '{dlq_name}' and DLX '{dlx_name}': {e}")
            raise

    async def aensure_delay_queue(
        self, delay_queue_name: str, target_queue_name: str, ttl_ms: int
    ) -> aio_pika.abc.AbstractQueue:
        """Ensure a delay queue that routes messages to the target queue after a TTL. Idempotent operation.

        Parameters
        ----------
        delay_queue_name : str
            The name of the delay queue to ensure.
        target_queue_name : str
            The name of the target queue where messages will be routed after the delay.
        ttl_ms : int
            The time-to-live in milliseconds for messages in the delay queue.

        Returns
        -------
        aio_pika.abc.AbstractQueue
            The declared delay queue.
        """
        if self.channel is None:
            logger.debug("Channel not established, connecting...")
            await self.aconnect()

        if self.channel is None:
            raise RuntimeError("[-] Failed to establish channel connection")

        try:
            arguments: dict[str, Any] = {
                "x-dead-letter-exchange": "",  # Default exchange
                "x-dead-letter-routing-key": target_queue_name,
                "x-message-ttl": ttl_ms,
            }
            delay_queue = await self.channel.declare_queue(
                delay_queue_name,
                durable=True,
                arguments=arguments,
            )
            logger.info(f"Delay queue '{delay_queue_name}' ensured with TTL {ttl_ms}ms")
            return delay_queue

        except aio_pika.exceptions.AMQPException as e:
            logger.error(f"Failed to declare delay queue '{delay_queue_name}': {e}")
            raise

    async def aensure_topic(
        self,
        topic_name: str,
        durable: bool = True,
    ) -> aio_pika.abc.AbstractExchange:
        """Ensure topic exchange exists with specified settings. Idempotent operation.

        Parameters
        ----------
        topic_name : str
            The name of the topic exchange to ensure.
        durable : bool, optional
            Whether the exchange should persist after broker restart, by default True.

        Returns
        -------
        aio_pika.abc.AbstractExchange
            The declared topic exchange.
        """
        if self.channel is None:
            logger.debug("Channel not established, connecting...")
            await self.aconnect()

        if self.channel is None:
            raise RuntimeError("[-] Failed to establish channel connection")

        try:
            exchange = await self.channel.declare_exchange(
                topic_name, aio_pika.ExchangeType.TOPIC, durable=durable
            )
            logger.debug(f"Topic exchange '{topic_name}' ensured")
            return exchange

        except aio_pika.exceptions.AMQPException as e:
            logger.error(f"Failed to declare topic exchange '{topic_name}': {e}")
            raise

    @asynccontextmanager
    async def aconnection_context(self) -> AsyncGenerator[None, None]:
        """Async context manager for RabbitMQ connection.

        Yields
        ------
        AsyncGenerator[None, None]
            Yields control to the context block with an active connection.

        Usage
        -----
            producer = RabbitMQProducer(config, url)
            producer.ensure_queue("my_queue")
            async with producer.connection_context():
                await producer.publish(message, queue_name)
        """
        try:
            await self.aconnect()
            yield

        finally:
            await self.adisconnect()
