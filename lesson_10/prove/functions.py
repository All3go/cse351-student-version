"""
Course: CSE 351, week 10
File: functions.py
Author: Austin Linford
Purpose: Functions for Family Search

Instructions:

Depth First Search
https://www.youtube.com/watch?v=9RHO6jU--GU

Breadth First Search
https://www.youtube.com/watch?v=86g8jAQug04


Requesting a family from the server:
family_id = 6128784944
data = get_data_from_server('{TOP_API_URL}/family/{family_id}')

Example JSON returned from the server
{
    'id': 6128784944, 
    'husband_id': 2367673859,        # use with the Person API
    'wife_id': 2373686152,           # use with the Person API
    'children': [2380738417, 2185423094, 2192483455]    # use with the Person API
}

Requesting an individual from the server:
person_id = 2373686152
data = get_data_from_server('{TOP_API_URL}/person/{person_id}')

Example JSON returned from the server
{
    'id': 2373686152, 
    'name': 'Stella', 
    'birth': '9-3-1846', 
    'parent_id': 5428641880,   # use with the Family API
    'family_id': 6128784944    # use with the Family API
}


--------------------------------------------------------------------------------------
You will lose 10% if you don't detail your part 1 and part 2 code below

Describe how to speed up part 1

For part 1, I kept the normal recursive DFS structure, but made it faster by
fetching all the people in each family at the same time using threads. Every family
has a husband, wife, and kids, and normally you would wait for each of those
requests one at a time. By using a small thread pool inside each DFS step, all of
those person requests happen in parallel. This keeps the code simple and still
speeds things up because the server calls are the slowest part.

Describe how to speed up part 2

For part 2, I used a BFS approach with a queue. To speed it up, I used threads to
process multiple families at once. Each worker thread fetches a family, then uses
the same idea as part 1 to fetch all the people in that family at the same time.
Any parent family IDs discovered are added back into the queue. This lets BFS move
forward level by level while still taking advantage of parallel server requests.

--------------------------------------------------------------------------------------
"""

from common import *
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------
# Helper functions to fetch data from server
# ---------------------------------------------------------------------

def _fetch_family(family_id):
    """Fetches a family object from the server. Returns Family or None."""
    data = get_data_from_server(f'{TOP_API_URL}/family/{family_id}')
    if data is None:
        return None
    return Family(data)

def _fetch_person(person_id):
    """Fetches a person object from the server. Returns Person or None."""
    data = get_data_from_server(f'{TOP_API_URL}/person/{person_id}')
    if data is None:
        return None
    return Person(data)

# ---------------------------------------------------------------------
# PART 1 — Depth First Search (recursive) with threaded person fetching
# ---------------------------------------------------------------------

def depth_fs_pedigree(family_id, tree):
    visited_families = set()
    visited_people = set()
    lock = threading.Lock()

    def dfs(fid):
        # Avoid duplicates
        with lock:
            if fid in visited_families:
                return
            visited_families.add(fid)

        family = _fetch_family(fid)
        if family is None:
            return

        # Add this family to the tree
        tree.add_family(family)

        # Collect person IDs in this family
        person_ids = []
        if family.get_husband() is not None:
            person_ids.append(family.get_husband())
        if family.get_wife() is not None:
            person_ids.append(family.get_wife())
        for c in family.get_children():
            person_ids.append(c)

        # Fetch all people in parallel
        futures = {}
        with ThreadPoolExecutor() as exec:
            for pid in person_ids:
                with lock:
                    if pid in visited_people:
                        continue
                    visited_people.add(pid)
                futures[exec.submit(_fetch_person, pid)] = pid

            for future in as_completed(futures):
                person = future.result()
                if person is not None:
                    tree.add_person(person)

        # After people are added, recurse DFS on parents
        husband = tree.get_person(family.get_husband())
        if husband is not None:
            parent = husband.get_parentid()
            if parent is not None:
                dfs(parent)

        wife = tree.get_person(family.get_wife())
        if wife is not None:
            parent = wife.get_parentid()
            if parent is not None:
                dfs(parent)

    dfs(family_id)

# ---------------------------------------------------------------------
# PART 2 — Breadth First Search (queue) with threaded family processing
# ---------------------------------------------------------------------

def breadth_fs_pedigree(family_id, tree):
    fam_queue = queue.Queue()
    fam_queue.put(family_id)

    visited_families = set()
    visited_people = set()
    lock = threading.Lock()

    def process_family(fid):
        # Skip if already processed
        with lock:
            if fid in visited_families:
                return []
            visited_families.add(fid)

        family = _fetch_family(fid)
        if family is None:
            return []

        tree.add_family(family)

        # Collect person IDs
        person_ids = []
        if family.get_husband() is not None:
            person_ids.append(family.get_husband())
        if family.get_wife() is not None:
            person_ids.append(family.get_wife())
        for c in family.get_children():
            person_ids.append(c)

        parent_families = []

        # Fetch people in parallel
        futures = {}
        with ThreadPoolExecutor() as exec:
            for pid in person_ids:
                with lock:
                    if pid in visited_people:
                        continue
                    visited_people.add(pid)
                futures[exec.submit(_fetch_person, pid)] = pid

            for future in as_completed(futures):
                person = future.result()
                if person is not None:
                    tree.add_person(person)
                    parent = person.get_parentid()
                    if parent is not None:
                        parent_families.append(parent)

        return parent_families

    # Thread pool for processing families in parallel
    with ThreadPoolExecutor() as exec:
        future_map = {}

        while not fam_queue.empty() or future_map:
            # Submit new families when available
            while not fam_queue.empty():
                fid = fam_queue.get()
                with lock:
                    if fid in visited_families:
                        continue
                f = exec.submit(process_family, fid)
                future_map[f] = fid

            # Wait for at least one to finish
            done, _ = _wait_for_one(future_map)

            # Add parent families to queue
            for f in done:
                pf = future_map.pop(f)
                result = f.result()
                for new_fid in result:
                    with lock:
                        if new_fid not in visited_families:
                            fam_queue.put(new_fid)

# ---------------------------------------------------------------------
# Helper: waits for ANY future to finish
# ---------------------------------------------------------------------

def _wait_for_one(future_map):
    from concurrent.futures import wait, FIRST_COMPLETED
    done, not_done = wait(list(future_map.keys()), return_when=FIRST_COMPLETED)
    return done, not_done
