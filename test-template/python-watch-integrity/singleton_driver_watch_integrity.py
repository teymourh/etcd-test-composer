#!/usr/bin/env -S python3 -u

# This file serves as a singleton driver (https://antithesis.com/docs/test_templates/test_composer_reference#singleton-driver-command). 
# This driver is used to test the integrity of the watch stream. It does this by writing 50 values randomly to etcd hosts in the cluster.
# Watcher connection is made to a fixed etcd host and watches for those values.
# It will assert that the value was received in the correct order and that all 50 values were received.

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
    unreachable,
)

from antithesis.random import (
 
    random_choice,
)

import etcd3
import time
import threading
import sys
import random, string


# Configuration
TEST_KEY = "/antithesis/watch-test"
TOTAL_UPDATES = 50

def connect_to_host():
    host = random_choice(["etcd0", "etcd1", "etcd2"])
    try:
        client = etcd3.client(host=host, port=2379)
        print(f"Client [watch-integrity]: connection successful with {host}")
        return client
    except Exception as e:
        # Antithesis Assertion: client should always be able to connect to an etcd host
        unreachable("Client failed to connect to an etcd host", {"host":host, "error":e})
        print(f"Client: failed to connect to {host}.")
        # sys.exit(1)

def put_request(c, key, value):
    try:
        c.put(key, value)
        return True, None
    except Exception as e:
        return False, e

def run_watch_integrity_test():
    #watcher connection 
    watcher_conn = etcd3.client(host='etcd0', port=2379)
    # State tracking for the assertion
    observed_values = []
    
    def watch_worker():
        # 1. Start the watch
        events_iterator, cancel = watcher_conn.watch(TEST_KEY)
        print(f"Client [watch-integrity]: Watcher started on {TEST_KEY}")
        
        expected_value = 1
        try:
            for event in events_iterator:
                current_val = int(event.value.decode('utf-8'))
                observed_values.append(current_val)
                print(f"Client [watch-integrity]: Watcher received value {current_val} for {TEST_KEY}")
                # ASSERTION: The value must always be exactly what we expect next (no skips/drops)
                # This validates that the stream is continuous and ordered.
                always(
                    current_val == expected_value, 
                    "Watch events must be sequential and contiguous",
                    {"expected": expected_value, "actual": current_val}
                )
                
                expected_value += 1
                if expected_value > TOTAL_UPDATES:
                    break
        finally:
            cancel()

    # 2. Start Watcher in a background thread
    watcher_thread = threading.Thread(target=watch_worker)
    watcher_thread.start()
    
    # Small sleep to ensure watcher is established
    time.sleep(1)

    # 3. Writer: Perform sequential updates
    for i in range(1, TOTAL_UPDATES + 1):
        writer_conn = connect_to_host()
        success, error = put_request(writer_conn, TEST_KEY, str(i))
        print(f"Client [watch-integrity]: Write attempt {i} on {TEST_KEY}")
        # Sometimes assertion: Ensure we actually hit a retry/network blip
        sometimes(success, "Successful write performed during test", {"error":error})
        writer_conn.close()

    watcher_thread.join(timeout=10)

    # Final check: Did we reach the end?
    always(
        len(observed_values) == TOTAL_UPDATES, 
        "Watcher must receive all published events",
        {"received": len(observed_values), "total": TOTAL_UPDATES}
    )
    # always(values_stay_consistent, "Database key values stay consistent", {"mismatch":mismatch})
if __name__ == "__main__":
    run_watch_integrity_test()
