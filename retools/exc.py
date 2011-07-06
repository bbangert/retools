"""retools exceptions"""

class RetoolsException(BaseException):
    """retools package base exception"""


class CacheConfigurationError(RetoolsException):
    """Raised when there's a cache configuration error"""
