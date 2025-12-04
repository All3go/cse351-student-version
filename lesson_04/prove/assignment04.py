"""
Course    : CSE 351
Assignment: 04
Student   : Austin Linford

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
from common import *
import threading
import queue
from cse351 import *

THREADS = 10                 # TODO - set for your program
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------
def retrieve_weather_data(command_queue, worker_queue):
    while True:
        command = command_queue.get()
        if command is None:
            command_queue.task_done()
            break

        city, record_no = command
        url = f'{TOP_API_URL}/record/{city}/{record_no}'
        data = get_data_from_server(url)
        worker_queue.put((city, data['date'], data['temp']))
        command_queue.task_done()


# ---------------------------------------------------------------------------
class Worker(threading.Thread):

    def __init__(self, worker_queue, noaa):
        super().__init__(daemon=True)
        self.worker_queue = worker_queue
        self.noaa = noaa
    
    def run(self):
        while True:
            item = self.worker_queue.get()
            if item is None:
                self.worker_queue.task_done()
                break

            city, date, temp = item
            self.noaa.add_record(city, date, temp)
            self.worker_queue.task_done()

# ---------------------------------------------------------------------------
# TODO - Complete this
class NOAA:

    def __init__(self):
        self.lock = threading.Lock()
        self.data = {city: [] for city in CITIES}

    def add_record(self, city, date, temp):
        with self.lock:
            self.data[city].append((date, temp))

    def get_temp_details(self, city):
        with self.lock:
            temps = [t for (_d, t) in self.data[city]]
            if not temps:
                return 0.0
        return sum(temps) / len(temps)


# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]['records']:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need
    command_queue = queue.Queue(maxsize=10)
    worker_queue = queue.Queue(maxsize=10)

    workers = []
    for _ in range(WORKERS):
        w = Worker(worker_queue, noaa)
        w.start()
        workers.append(w)
    
    retrivers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(command_queue, worker_queue))
        t.start()
        retrivers.append(t)
    
    for city in CITIES:
        for record_no in range(RECORDS_TO_RETRIEVE):
            command_queue.put((city, record_no))
    
    for _ in range(THREADS):
        command_queue.put(None)

    for t in retrivers:
        t.join()
    
    for _ in range(WORKERS):
        worker_queue.put(None)

    for w in workers:
        w.join()


    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()

