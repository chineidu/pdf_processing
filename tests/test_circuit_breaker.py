"""Tests for src/utilities/circuit_breaker.py"""

import time

from src.schemas.types import CircuitBreakerStateEnum
from src.utilities.circuit_breaker import CircuitBreaker


class TestCircuitBreakerInit:
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitBreakerStateEnum.CLOSED

    def test_initial_failure_count_is_zero(self):
        cb = CircuitBreaker()
        assert cb.failure_count == 0

    def test_initial_last_failure_time_is_none(self):
        cb = CircuitBreaker()
        assert cb.last_failure_time is None

    def test_custom_failure_threshold(self):
        cb = CircuitBreaker(failure_threshold=5)
        assert cb.failure_threshold == 5

    def test_custom_recovery_timeout(self):
        cb = CircuitBreaker(recovery_timeout=120)
        assert cb.recovery_timeout == 120


class TestCircuitBreakerCanExecute:
    def test_can_execute_when_closed(self):
        cb = CircuitBreaker()
        assert cb.can_execute() is True

    def test_cannot_execute_when_open(self):
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure()
        assert cb.state == CircuitBreakerStateEnum.OPEN
        assert cb.can_execute() is False

    def test_transitions_to_half_open_after_recovery_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0)
        cb.record_failure()
        assert cb.state == CircuitBreakerStateEnum.OPEN
        # recovery_timeout=0 means it should immediately be eligible
        time.sleep(0.01)
        result = cb.can_execute()
        assert result is True
        assert cb.state == CircuitBreakerStateEnum.HALF_OPEN


class TestCircuitBreakerRecordFailure:
    def test_increments_failure_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        assert cb.failure_count == 1

    def test_sets_last_failure_time(self):
        cb = CircuitBreaker()
        cb.record_failure()
        assert cb.last_failure_time is not None

    def test_trips_to_open_at_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure()
        assert cb.state == CircuitBreakerStateEnum.OPEN

    def test_stays_closed_below_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitBreakerStateEnum.CLOSED

    def test_failure_in_half_open_reverts_to_open(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0)
        cb.record_failure()  # trips to OPEN
        time.sleep(0.01)
        cb.can_execute()  # transitions to HALF_OPEN
        assert cb.state == CircuitBreakerStateEnum.HALF_OPEN
        cb.record_failure()  # should revert to OPEN
        assert cb.state == CircuitBreakerStateEnum.OPEN


class TestCircuitBreakerRecordSuccess:
    def test_resets_failure_count_in_closed_state(self):
        cb = CircuitBreaker(failure_threshold=5)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        assert cb.failure_count == 0

    def test_resets_to_closed_from_half_open(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0)
        cb.record_failure()
        time.sleep(0.01)
        cb.can_execute()  # -> HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitBreakerStateEnum.CLOSED
        assert cb.failure_count == 0

    def test_success_in_open_state_does_not_change_state(self):
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure()
        assert cb.state == CircuitBreakerStateEnum.OPEN
        cb.record_success()
        # Success in OPEN state should not reset the circuit breaker
        assert cb.state == CircuitBreakerStateEnum.OPEN
