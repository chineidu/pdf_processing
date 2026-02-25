import time
from typing import TYPE_CHECKING

from src import create_logger
from src.schemas.types import CircuitBreakerStateEnum

if TYPE_CHECKING:
    pass

logger = create_logger(name=__name__)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 60) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.state: CircuitBreakerStateEnum = CircuitBreakerStateEnum.CLOSED
        self.last_failure_time: float | None = None
        logger.info(
            f"Circuit Breaker initialized with failure_threshold={self.failure_threshold}, "
            f"recovery_timeout={self.recovery_timeout} seconds."
        )

    def record_failure(self) -> None:
        """Record a failure, update the circuit breaker state and last failure time"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerStateEnum.OPEN
            logger.error(
                f"🔴 Circuit Breaker tripped ({CircuitBreakerStateEnum.CLOSED.name} -> "
                f"{CircuitBreakerStateEnum.OPEN.name}). Not accepting requests."
            )

        if self.state == CircuitBreakerStateEnum.HALF_OPEN:
            self.state = CircuitBreakerStateEnum.OPEN
            logger.error(
                f"🔴 Circuit Breaker reverted to {CircuitBreakerStateEnum.OPEN.name} "
                f"after failure in {CircuitBreakerStateEnum.HALF_OPEN.name} state."
            )

    def record_success(self) -> None:
        """Record a success and reset the circuit breaker if in HALF_OPEN state"""
        if self.state == CircuitBreakerStateEnum.HALF_OPEN:
            logger.info(
                f"🟢 Circuit Breaker reset ({CircuitBreakerStateEnum.HALF_OPEN.name} -> "
                f"{CircuitBreakerStateEnum.CLOSED.name}). Accepting requests."
            )

            # Clear count
            self.failure_count = 0
            self.state = CircuitBreakerStateEnum.CLOSED

        elif self.state == CircuitBreakerStateEnum.CLOSED:
            # In CLOSED state, just reset failure count on success
            self.failure_count = 0

    def can_execute(self) -> bool:
        """Check if requests can be executed based on the circuit breaker state"""
        if self.state == CircuitBreakerStateEnum.CLOSED:
            return True

        if (
            self.state == CircuitBreakerStateEnum.OPEN
            and self.last_failure_time is not None
            and (time.time() - self.last_failure_time > self.recovery_timeout)
        ):
            self.state = CircuitBreakerStateEnum.HALF_OPEN
            logger.warning(
                f"🟠 Circuit Breaker is {CircuitBreakerStateEnum.HALF_OPEN.name}. Testing recovery..."
            )
            return True

        return self.state == CircuitBreakerStateEnum.CLOSED
