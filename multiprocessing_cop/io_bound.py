import sys
import threading
import concurrent.futures
import requests
import time
import multiprocessing


"""
Linear
"""


def download_site(url, session):
    with session.get(url) as response:
        print(f"Downloaded {len(response.content)} lines from {url}")


def download_multiple(site_list):
    with requests.Session() as session:
        for url in site_list:
            download_site(url, session)


"""
Threading
"""

thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()


def download_site_threads(url):
    session = get_session()
    with session.get(url) as response:
        print(f"Downloaded {len(response.content)} lines from {url}")


def download_multiple_threads(site_list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        thread = executor.submit(download_site_threads, site_list)


"""
Multiprocessing
"""
session = None


def set_global_session():
    global session
    if not session:
        session = requests.Session()


def download_site_multiprocessing(url):
    with session.get(url) as response:
        print(f"Downloaded {len(response.content)} lines from {url}")


def download_multiple_multiprocessing(site_list):
    with multiprocessing.Pool(initializer=set_global_session) as pool:
        pool.map(download_site_multiprocessing, site_list)


def io_bound_task(concurrent=0):
    site_list = [
        "https://joyceproject.com/index.php?chapter=" + chap
        for chap in [
            "telem",
            "nestor",
            "proteus",
            "calypso",
            "lotus",
            "hades",
            "aeolus",
            "lestry",
            "scylla",
            "wrocks",
            "sirens",
            "cyclops",
            "nausicaa",
            "oxen",
            "circe",
            "eumaeus",
            "ithaca",
            "penelope",
        ]
    ] * 5
    start_time = time.time()
    fn_dict = {
        0: download_multiple,
        1: download_multiple_threads,
        2: download_multiple_multiprocessing,
    }
    download_fn = fn_dict[concurrent]
    download_fn(site_list)
    duration = time.time() - start_time
    print(f"Downloaded {len(site_list)} sites in {duration} s.")


if __name__ == "__main__":
    try:
        use_concurrent = int(sys.argv[1])
    except IndexError:
        use_concurrent = 0
    io_bound_task(use_concurrent)
