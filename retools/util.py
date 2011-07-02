"""Utility functions"""
import inspect

def func_namespace(func):
    """Generates a unique namespace for a function"""
    kls = None
    if hasattr(func, 'im_func'):
        kls = func.im_class
        func = func.im_func

    if kls:
        return '%s.%s' % (kls.__module__, kls.__name__)
    else:
        return '%s.%s' % (func.__module__, func.__name__)


def has_self_arg(func):
    """Return True if the given function has a 'self' argument."""
    return inspect.getargspec(func)[0][0] in ('self', 'cls')


def with_nested_contexts(context_managers, func, kwargs):
    """Nested context manager calling
    
    Given a function, and keyword arguments to call it with, it will
    be wrapped in a with statment using every context manager in the
    context_managers list for nested with calling.
    
    Every context_manager will get the function reference, and keyword
    arguments.
    
    Example::
        
        with ContextA(func, **kwargs):
            with ContextB(func, **kwargs):
                return func(**kwargs)
        
        # is equivilant to
        ctx_managers = [ContextA, ContextB]
        return with_nested_contexts(ctx_managers, func, kwargs)
    
    """
    if not context_libs:
        return func(*kwargs)
    else:
        ctx_manager = context_managers[0]
        with ctx_manager(**kwargs):
            return with_nested_contexts(context_managers[1:], func, kwargs)
