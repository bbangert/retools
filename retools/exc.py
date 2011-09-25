"""retools exceptions"""


class RetoolsException(BaseException):
    """retools package base exception"""


class ConfigurationError(RetoolsException):
    """Raised for general configuration errors"""


class CacheConfigurationError(RetoolsException):
    """Raised when there's a cache configuration error"""


class QueueError(RetoolsException):
    """Raised when there's an error in the queue code"""


class AbortJob(RetoolsException):
    """Raised to abort execution of a job"""


class UnregisteredJob(RetoolsException):
    """Raised when attempting to enqueue a job that isn't registered"""
