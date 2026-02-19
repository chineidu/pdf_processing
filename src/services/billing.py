from decimal import Decimal

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from sqlalchemy import func, update

from src import create_logger
from src.db.models import DBAPIKey, DBClient, aget_db

logger = create_logger(name=__name__)
tracer = trace.get_tracer(__name__)


async def adeduct_credits_background(
    client_id: int, key_id: int, cost: float | Decimal
) -> None:
    """
    Deduct credits from a client's account and update API key usage.
    Intended to be run as a background task with distributed tracing.
    """
    cost_value = float(cost)

    with tracer.start_as_current_span("billing.deduct_credits") as span:
        # Save attributes for observability
        span.set_attribute("billing.client_id", client_id)
        span.set_attribute("billing.key_id", key_id)
        span.set_attribute("billing.cost", cost_value)

        async for session in aget_db():
            try:
                # Task 1: Update Timestamp
                stmt_key = (
                    update(DBAPIKey)
                    .where(DBAPIKey.id == key_id)
                    .values(last_used_at=func.now())
                )
                await session.execute(stmt_key)

                # Task 2: Deduct Credits (if cost > 0)
                if cost_value > 0:
                    stmt_bill = (
                        update(DBClient)
                        .where(DBClient.id == client_id)
                        .values(credits=DBClient.credits - cost_value)
                    )
                    span.add_event("credits_deducted", {"amount": cost_value})
                    await session.execute(stmt_bill)

                # Commit both changes in a single transaction
                # This ensures atomicity - either both succeed or both fail
                await session.commit()
                logger.debug(
                    f"Billing Success: Client {client_id} | Cost {cost_value:.2f}"
                )
                span.add_event("billing_success")
                span.set_status(Status(StatusCode.OK))

            except Exception as e:
                logger.error(f"Billing Failed for Client {client_id}: {e}")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

            # Break generator to close session
            break
