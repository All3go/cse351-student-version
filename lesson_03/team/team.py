"""
Course: CSE 351 
Lesson: L03 team activity
File:   team.py
Author: <Add name here>

Purpose: Retrieve Star Wars details from a server (step 3 - multiple workers for speed)
"""

import threading
import queue
from common import *

from cse351 import *

# global
call_count = 0

def get_name(url):
    global call_count
    item = get_data_from_server(url)
    call_count += 1
    if item is None:
        return None
    return item.get('name') or item.get('title')

def worker(q):
    """
    Worker thread: pulls URLs from the queue until empty, retrieves names, prints them.
    """
    while True:
        try:
            url = q.get_nowait()
        except queue.Empty:
            break
        name = get_name(url)
        print(f'  - {name}')
        q.task_done()

def main():
    global call_count

    log = Log(show_terminal=True)
    log.start_timer('Starting to retrieve data from the server')

    film6 = get_data_from_server(f'{TOP_API_URL}/films/6')
    call_count += 1
    print("Film 6 loaded!")

    # Gather all URLs (characters, planets, species, vehicles, starships)
    urls = (
        film6['characters'] +
        film6['planets'] +
        film6['species'] +
        film6['vehicles'] +
        film6['starships']
    )

    # Create queue and fill it
    q = queue.Queue()
    for url in urls:
        q.put(url)

    # Start multiple worker threads
    threads = []
    num_threads = 10  # you can experiment with this number
    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(q,))
        t.start()
        threads.append(t)

    # Wait until all tasks are done
    q.join()

    # Wait for all worker threads to finish
    for t in threads:
        t.join()

    log.stop_timer('Total Time To complete')
    log.write(f'There were {call_count} calls to the server')

if __name__ == "__main__":
    main()
