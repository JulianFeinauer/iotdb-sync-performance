import os
import random
import subprocess
import sys
import threading
import time

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.Tablet import Tablet

ip = "127.0.0.1"
port_ = os.environ.get("PORT", "7001")
username_ = 'root'
password_ = 'root'


def insert(device):
    session = Session(ip, port_, username_, password_)
    session.open(False)
    timestamp = 0
    for epochs in range(0, 10):
        print(f"[{device}] Epoch {epochs}")
        for _ in range(0, 10000):
            session.insert_str_record(f"root.{device}", timestamp, ["temp"],
                                      [str(random.uniform(0, 100))])
            timestamp = timestamp + 1
        # session.execute_non_query_statement("FLUSH;")

    session.close()


def insert_tablets(device):
    session = Session(ip, port_, username_, password_)
    session.open(False)
    for epochs in range(0, 100):
        print(f"[{device}] Epoch {epochs}")
        for _ in range(0, 100):
            timestamps = []
            values = []
            for _ in range(0, 100):
                timestamps.append(int(time.time() * 1000))
                values.append([random.uniform(0, 100)])

            session.insert_tablet(Tablet(f"root.{device}", ["temp"], [TSDataType.FLOAT], values, timestamps))
        session.execute_non_query_statement("FLUSH;")

    session.close()


def launch_process(device_name):
    proc = subprocess.Popen(["python", "main.py", device_name], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    while True:
        line = proc.stdout.readline()
        if not line:
            break

        print(line.decode("ascii"), end="")

def start(device, workers):
    print("Running the Supervisor process, spawning 10 workers...")
    for device_id in range(0, int(workers)):
        device_name = f"device-{device}"
        t = threading.Thread(target=launch_process, args=(device_name,))
        t.setDaemon(False)
        t.start()

if __name__ == '__main__':
    if len(sys.argv) == 4:
        print("Running the Supervisor process")
        port_ = sys.argv[1]
        worker = sys.argv[2]
        device = sys.argv[3]
        insert(f"device-{device}")
        # print(f"Sending to port {port_} with {worker} workers and device id {device}")
        # start(device, worker)
    # if len(sys.argv) == 1:
    #     print("Running the Supervisor process, spawning 10 workers...")
    #     for device_id in range(0, 10):
    #         device_name = f"device-2-{device_id}"
    #         t = threading.Thread(target=launch_process, args=(device_name,))
    #         t.setDaemon(False)
    #         t.start()
    if len(sys.argv) == 2:
        insert(sys.argv[-1])
