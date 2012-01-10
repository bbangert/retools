import json

def simplemath(arg1=0, arg2=2):
    return arg1 + arg2


# Return the result after it runs
def return_result(job=None, result=None):
    pl = job.redis.pipeline()
    result = json.dumps({'data': result})
    pl.lpush('retools:result:%s' % job.job_id, result)
    pl.expire('retools:result:%s', 3600)
    pl.execute()

# If it fails, return a 'ERROR'
def return_failure(job=None, exc=None):
    pl = job.redis.pipeline()
    exc = json.dumps({'data': 'ERROR: %s' % exc})
    pl.lpush('retools:result:%s' % job.job_id, exc)
    pl.expire('retools:result:%s', 3600)
    pl.execute()


def add_events(qm):
    qm.subscriber('job_postrun', 'retools.jobs:simplemath',
                  handler='retools.jobs:return_result')
    qm.subscriber('job_failure', 'retools.jobs:simplemath',
                  handler='retools.jobs:return_failure')

def wait_for_result(qm, job, **kwargs):
    job_id = qm.enqueue(job, **kwargs)
    result = qm.redis.blpop('retools:result:%s' % job_id)[1]
    return json.loads(result)['data']
