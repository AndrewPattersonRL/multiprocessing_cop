import sys
import concurrent.futures
import multiprocessing
import threading
import math
import time
import itertools as it
from joblib import Parallel, delayed


"""
Getting primes between tow limit values.
"""
def get_prime_numbers(limit):
    begin, end = limit
    primes = []
    if begin <= 2:
        primes.append(2)
        if end > 2:
            begin = 3
        else:
            return primes if end == 2 else []

    begin += int(not begin % 2)

    for idx in range(begin, end + 1, 2):
        sqrt_idx = int(idx**0.5) + 1
        if not any(idx % j == 0 for j in range(3, sqrt_idx, 2)):
            primes.append(idx)

    return primes

"""
Getting primes with the more efficient sieve method
"""
def sieve_of_eratosthenes_sliced(end, n_workers=None):
    reg = [True] * (end + 1)
    reg[0] = reg[1] = False
    reg[4::2] = [False] * len(reg[4::2])

    for idx in range(3, int(end**0.5) + 1, 2):
        if reg[idx]:
            reg[idx * idx :: 2 * idx] = [False] * len(reg[idx * idx :: 2 * idx])
    yield from filter(lambda x: reg[x], range(end + 1))


def sieve_of_eratosthenes_looped(end, n_workers=None):
    reg = [True] * (end + 1)
    reg[0] = reg[1] = False
    for r in range(4, len(reg), 2):
        reg[r] = False

    for idx in range(3, int(end**0.5) + 1, 2):
        if reg[idx]:
            for r in range(idx * idx, len(reg), 2 * idx):
                reg[r] = False 
    yield from filter(lambda x: reg[x], range(end + 1))


def return_start_end_points(end, n_splits):
    if n_splits == 1:
        return zip([0], [end])
    search_spaces = list(range(0, end, int(end / n_splits)))
    search_spaces[-1] = end
    return zip(
        [search_spaces[0]] + [x + 1 for x in search_spaces[1:]], search_spaces[1:]
    )

"""
Threading Method
"""
def get_primes_threading(end, n_threads):
    start_ends = return_start_end_points(end, n_threads)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        primes = executor.map(get_prime_numbers, start_ends)
    return list(it.chain.from_iterable(primes))

"""
Multiprocessing
"""
def get_primes_multiprocessing(end, n_workers):
    start_ends = return_start_end_points(end, n_workers)
    with multiprocessing.Pool(processes=n_workers) as pool:
        primes = pool.map(get_prime_numbers, start_ends)
    return list(it.chain.from_iterable(primes))

"""
Joblib
"""
def get_primes_joblib(end, n_workers):
    start_ends = return_start_end_points(end, n_workers)
    primes = Parallel(n_jobs=n_workers, )(
        delayed(get_prime_numbers)(limit) for limit in list(start_ends)
    )
    return list(it.chain.from_iterable(primes))


def print_results(fn, fn_name, end, n_workers):
    start = time.time()
    n_primes = len(list(fn(end, n_workers)))
    print(f"{n_primes} primes found")
    print(f"{fn_name} time: {time.time() - start} s.")


if __name__ == "__main__":
    [end, n_workers] = [int(float(x)) for x in sys.argv[1:3]]
    fn_names = sys.argv[3:]
    if len(fn_names) == 0:
        fn_names = "all"

    fn_dict = {
        "sieve_sliced": sieve_of_eratosthenes_sliced,
        "sieve_looped": sieve_of_eratosthenes_looped,
        "threading": get_primes_threading,
        "multiprocessing": get_primes_multiprocessing,
        "joblib": get_primes_joblib,
    }

    if fn_names == "all":
        for name, fn in fn_dict.items():
            print_results(fn, name, end, n_workers)
    else:
        for name in fn_names:
            print_results(fn_dict[name], name, end, n_workers)
