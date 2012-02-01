import unittest

from contextlib import contextmanager

from nose.tools import eq_


class TestNamespaceFunc(unittest.TestCase):
    def _makeKey(self, func, deco_args):
        from retools.util import func_namespace
        return func_namespace(func, deco_args)

    def test_func_name(self):
        def a_func(): pass
        eq_('retools.tests.test_util.a_func.', self._makeKey(a_func, []))

    def test_class_method_name(self):
        # This simulates the class method by checking for 'cls' arg
        eq_('retools.tests.test_util.DummyClass.',
            self._makeKey(DummyClass.class_method, []))


class TestContextManager(unittest.TestCase):
    def _call_with_contexts(self, ctx_managers, func, kwargs):
        from retools.util import with_nested_contexts
        return with_nested_contexts(ctx_managers, func, [], kwargs)

    def test_nest_call(self):
        def a_func(**kwargs):
            kwargs['list'].append('here')
            return kwargs['list']

        @contextmanager
        def ctx_a(func, *args, **kwargs):
            eq_(func, a_func)
            kwargs['list'].append(0)
            yield
            kwargs['list'].append(1)

        @contextmanager
        def ctx_b(func, *args, **kwargs):
            eq_(func, a_func)
            kwargs['list'].append(2)
            yield
            kwargs['list'].append(3)

        lst = []
        kwargs = dict(list=lst)
        result = self._call_with_contexts([ctx_a, ctx_b], a_func, kwargs)
        eq_([0, 2, 'here', 3, 1], result)


class DummyClass(object):  # pragma: nocover
    def class_method(cls):
        return arg
