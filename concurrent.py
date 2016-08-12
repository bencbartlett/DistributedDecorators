import multiprocessing as mp
import types


class Concurrent(object):

    def __init__(self, numProcesses = mp.cpu_count()):
        '''Initialization options go here.'''
        self.numProcesses = numProcesses

    def __call__(self, func, ordered = True):
        '''On-call options go here.'''
        if not (type(func) is types.FunctionType or type(func) is types.MethodType):
            raise TypeError("Evaluable object must be a function or method, " + str(type(func)) + " was passed.")

        def wrapper(self, *args, **kwargs):
            return func(*args, **kwargs)
