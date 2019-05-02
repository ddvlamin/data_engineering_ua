import time

def timeit(loops):
    def decorator(func):
        def inner(*args, **kwargs):
            times = list()
            for i in range(loops):
                start = time.time()
                rv = func(*args,**kwargs)
                end = time.time()
                times.append(end-start)
            avg_time = sum(times)/len(times)
            print("average execution time: {}".format(avg_time))
            return rv
        return inner
    return decorator
