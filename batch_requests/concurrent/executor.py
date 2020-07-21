'''
Created on Feb 20, 2016

@author: Rahul Tanwani
'''
from abc import ABCMeta
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures.process import ProcessPoolExecutor

from django.db import transaction

class Executor(object):
    '''
        Based executor class to encapsulate the job execution.
    '''
    __metaclass__ = ABCMeta

    def execute(self, requests, resp_generator, *args, **kwargs):
        '''
            Calls the resp_generator for all the requests in parallel in an asynchronous way.
        '''
        result_futures = [self.executor_pool.submit(resp_generator, req, *args, **kwargs) for req in requests]
        resp = [res_future.result() for res_future in result_futures]
        return resp


class SequentialExecutor(Executor):
    '''
        Executor for executing the requests sequentially.
    '''

    def execute(self, requests, resp_generator, *args, **kwargs):
        '''
            Calls the resp_generator for all the requests in sequential order.
        '''
        return [resp_generator(request) for request in requests]


class AtomicExecutor(Executor):
    '''
        Sequential executor that rolls back the transaction if there's an error.
    '''

    def execute(self, requests, resp_generator, *args, **kwargs):
        '''
            Calls the resp_generator for all the requests in a single transaction.
        '''
        response = []
        try:
            with transaction.atomic():
                for request in requests:
                    r = resp_generator(request)
                    response.append(r)
                    if r["status_code"] >= 400:
                        raise ValueError()
        except ValueError:
            pass
        return response


class ParallelExecutor(Executor):
    pass


class ThreadBasedExecutor(ParallelExecutor):
    '''
        An implementation of executor using threads for parallelism.
    '''
    def __init__(self, num_workers):
        '''
            Create a thread pool for concurrent execution with specified number of workers.
        '''
        self.executor_pool = ThreadPoolExecutor(num_workers)


class ProcessBasedExecutor(ParallelExecutor):
    '''
        An implementation of executor using process(es) for parallelism.
    '''
    def __init__(self, num_workers):
        '''
            Create a process pool for concurrent execution with specified number of workers.
        '''
        self.executor_pool = ProcessPoolExecutor(num_workers)
