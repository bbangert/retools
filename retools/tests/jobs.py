from retools.queue import job

@job
def echo_default(default='hello'): return default

@job(category='test_cat')
def echo_back(): return 'howdy all'
