import os
import sys
import numpy as np
import time
import multiprocessing
from joblib import Parallel, delayed

from cpu_bound import return_start_end_points, print_results


"""
Getting the primes
"""
def get_primes_limit(limit):
    begin, end = limit
    primes = []
    if begin <= 2:
        primes.append(2)
        if end > 2:
            begin = 3

    begin += int(not begin % 2)

    for idx in range(begin, end + 1, 2):
        sqrt_idx = int(idx**0.5) + 1
        if not any(idx % j == 0 for j in range(3, sqrt_idx, 2)):
            primes.append(idx)
    return primes


def get_prime_numbers_shared(limit, shared_primes):
    primes = get_primes_limit(limit)
    shared_primes.put(primes)


"""
Multiprocessing
"""
def get_primes_multiprocessing_shared(end, n_workers):
    start_ends = return_start_end_points(end, n_workers)

    shared_primes = multiprocessing.Queue()

    processes = []
    for limit in start_ends:
        p = multiprocessing.Process(
            target=get_prime_numbers_shared, args=(limit, shared_primes)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    primes = []
    while not shared_primes.empty():
        primes.extend(shared_primes.get())
    return primes


joblib_primes = set()

def get_prime_numbers_joblib_single_process(limit):
    primes = get_primes_limit(limit)
    joblib_primes.update(primes)


def get_primes_joblib_shared(end, n_workers):
    start_ends = return_start_end_points(end, n_workers)
    _ = Parallel(n_jobs=n_workers, require="sharedmem")(
        delayed(get_prime_numbers_joblib_single_process)(limit) for limit in list(start_ends)
    )
    return list(joblib_primes)


def setup_large_file(file_size):
    large_matrix = np.random.rand(file_size)

    file_on_disk = np.memmap(
        "large_matrix.dat", dtype="complex64", mode="w+", shape=large_matrix.shape
    )

    file_on_disk[:] = large_matrix[:]
    del file_on_disk


def processing_fn(x):
    return x**0.5


def joblib_sqrt_matrix(file_size, n_workers):
    file_on_disk = np.memmap(
        "large_matrix.dat", dtype="complex64", mode="r", shape=(file_size,)
    )

    result = Parallel(n_jobs=n_workers)(delayed(processing_fn)(x) for x in file_on_disk)

    file_result = np.memmap(
        "large_result.dat", dtype="complex64", mode="w+", shape=(file_size,)
    )
    file_result[:] = result[:]

    del file_result


def cleanup():
    for fname in ["large_matrix.dat", "large_result.dat"]:
        try:
            os.remove(fname)
            os.sync()
        except FileNotFoundError as e:
            pass


def show_results(fn, name, param, n_workers):
    if name == "joblib_memmap":
        try:
            setup_large_file(param)
            start = time.time()
            fn(param, n_workers)
            print(f"{param} entries modified")
            print(f"{name} time: {time.time() - start} s.")
        except KeyboardInterrupt:
            print("Interrupted.")
        except Exception as e:
            pass
        finally:
            cleanup()
    else:
        print_results(fn, name, param, n_workers)


if __name__ == "__main__":
    [end, n_workers] = [int(float(eval(x))) for x in sys.argv[1:3]]
    fn_names = sys.argv[3:]
    if len(fn_names) == 0:
        fn_names = "all"

    fn_dict = {
        "multiprocessing": get_primes_multiprocessing_shared,
        "joblib": get_primes_joblib_shared,
        "joblib_memmap": joblib_sqrt_matrix,
    }

    if fn_names == "all":
        for name, fn in fn_dict.items():
            show_results(fn, name, end, n_workers)
    else:
        for name in fn_names:
            show_results(fn_dict[name], name, end, n_workers)
