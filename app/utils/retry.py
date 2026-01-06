"""
Retry utility with exponential backoff for handling transient failures.

Ensures reliability and fault tolerance in production environments.
"""

import time
import functools
from typing import Callable, Any, Optional, Tuple, Type
from app.core import get_logger

logger = get_logger(__name__)


def retry_with_backoff(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for retrying a function with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds (default: 1.0)
        backoff_factor: Multiplier for delay between retries (default: 2.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        exceptions: Tuple of exception types to catch and retry (default: all exceptions)

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(max_attempts=5, initial_delay=2.0)
        def unreliable_function():
            # May fail intermittently
            pass
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            delay = initial_delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"Function {func.__name__} succeeded on attempt {attempt}/{max_attempts}"
                        )
                    return result

                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts. "
                            f"Last error: {e}"
                        )
                        raise

                    logger.warning(
                        f"Function {func.__name__} failed on attempt {attempt}/{max_attempts}. "
                        f"Error: {e}. Retrying in {delay:.2f}s..."
                    )

                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class RetryableOperation:
    """
    Context manager for retryable operations with manual retry control.

    Use this when you need more control over retry logic than the decorator provides.
    """

    def __init__(
        self,
        operation_name: str,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
    ):
        """
        Initialize retryable operation.

        Args:
            operation_name: Name of the operation for logging
            max_attempts: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            backoff_factor: Multiplier for delay between retries
            max_delay: Maximum delay between retries
        """
        self.operation_name = operation_name
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay

        self.current_attempt = 0
        self.delay = initial_delay

    def should_retry(self, exception: Exception) -> bool:
        """
        Determine if operation should be retried.

        Args:
            exception: Exception that occurred

        Returns:
            bool: True if should retry, False otherwise
        """
        self.current_attempt += 1

        if self.current_attempt >= self.max_attempts:
            logger.error(
                f"Operation '{self.operation_name}' failed after "
                f"{self.max_attempts} attempts. Last error: {exception}"
            )
            return False

        logger.warning(
            f"Operation '{self.operation_name}' failed on attempt "
            f"{self.current_attempt}/{self.max_attempts}. "
            f"Error: {exception}. Retrying in {self.delay:.2f}s..."
        )

        time.sleep(self.delay)
        self.delay = min(self.delay * self.backoff_factor, self.max_delay)

        return True

    def reset(self):
        """Reset retry state for a new operation."""
        self.current_attempt = 0
        self.delay = self.initial_delay
