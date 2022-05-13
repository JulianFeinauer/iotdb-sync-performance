import atexit
import os.path
import random
import shutil
import subprocess
import threading
from time import sleep

from iotdb.Session import Session
from iotdb.utils.RowRecord import RowRecord
from iotdb.utils.SessionDataSet import SessionDataSet

STARTING_PORT = 7001

NUMBER_OF_EDGE_DEVICES = 40
NUMBER_OF_WORKER_THREADS = 1
RECORDS_PER_EPOCH = 100
EPOCHS = 1


def modify_file(filename, map_function):
    config_file = open(filename)
    config = config_file.read()
    config_file.close()

    config = map_function(config)

    config_file = open(filename, "w")
    config_file.write(config)
    config_file.close()


def modify_cloud_config(config):
    config = config.replace("# is_sync_enable=false", "is_sync_enable=true")
    config = config.replace("# sync_server_port=5555", "sync_server_port=5555")
    config = config.replace("# ip_white_list=0.0.0.0/0", "ip_white_list=0.0.0.0/0")
    return config


def modify_edge_config(config, port):
    config = config.replace("rpc_port=6667", f"rpc_port={port}")
    config = config.replace("# wal_buffer_size=16777216", "wal_buffer_size=1677721")
    config = config.replace("# enable_timed_flush_unseq_memtable=true", "enable_timed_flush_unseq_memtable=false")
    config = config.replace("# enable_timed_close_tsfile=true", "enable_timed_close_tsfile=false")
    config = config.replace("# storage_group_report_threshold=16777216", "storage_group_report_threshold=1677721")
    config = config.replace("# enable_unseq_space_compaction=true", "enable_unseq_space_compaction=false")
    return config


def modify_edge_env_config(config):
    config = config.replace('#MAX_HEAP_SIZE="2G"', 'MAX_HEAP_SIZE="100M"')
    config = config.replace('#HEAP_NEWSIZE="2G"', 'HEAP_NEWSIZE="100M"')
    config = config.replace('MAX_DIRECT_MEMORY_SIZE=${MAX_HEAP_SIZE}', 'MAX_DIRECT_MEMORY_SIZE="50M"')
    config = config.replace('threads_number="16"', 'threads_number="4"')
    return config

STOP = False


processes_spawned = []


def start_server(folder, start_command="sbin/start-server.sh"):
    proc = subprocess.Popen([start_command], cwd=folder, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    processes_spawned.append(proc)
    while True:
        line = proc.stdout.readline()
        if not line:
            break
        # print(f"[{folder}] {line.decode('ascii')}", end="")


def start_iotdb_server(path, start_command="sbin/start-server.sh"):
    print(f"Starting {start_command} in folder {path}")
    t = threading.Thread(target=start_server, args=(path, start_command))
    t.setDaemon(True)
    t.start()


def before_end_hook():
    print("Initiating Stop Process")
    for p in processes_spawned:
        p: subprocess.Popen
        print(f"Killing process {p.pid}")
        p.kill()


def insert(device, port):
    session = Session("localhost", port, "root", "root")
    session.open(False)
    timestamp = 0
    for epochs in range(0, EPOCHS):
        print(f"[{device}] Epoch {epochs}")
        for _ in range(0, RECORDS_PER_EPOCH):
            session.insert_str_record(f"root.{device}", timestamp, ["temp"],
                                      [str(random.uniform(0, 100))])
            timestamp = timestamp + 1
        session.execute_non_query_statement("FLUSH;")
    print(f"Insert for {device} is done!")
    session.close()


def insert_thread(device, port):
    print(f"Start insert Thread for {device} against port {port}")
    t = threading.Thread(target=insert, args=(device, port))
    t.setDaemon(False)
    t.start()


def start_insert_threads(port):
    # Wait for the server to be ready
    while True:
        try:
            session = Session("localhost", port, "root", "root")
            session.open(False)
            session.close()
            break
        except:
            # intentionally to nothing
            pass
        sleep(5)

    for i in range(0, NUMBER_OF_WORKER_THREADS):
        device = f"device-{port}-{i}"
        insert_thread(device, port)


if __name__ == '__main__':
    atexit.register(before_end_hook)

    if os.path.exists("tmp"):
        shutil.rmtree("./tmp")
    os.makedirs("tmp/edges")

    print("Creaing Cloud Instance...")
    shutil.copytree("files/iotdb", f"tmp/cloud")
    modify_file("tmp/cloud/conf/iotdb-engine.properties", modify_cloud_config)
    start_iotdb_server("tmp/cloud/")

    # Create all edge iotdbs
    for i in range(0, NUMBER_OF_EDGE_DEVICES):
        print(f"Creating Edge {i}")
        shutil.copytree("files/iotdb", f"tmp/edges/iotdb_{i}")
        port = STARTING_PORT + i
        modify_file(f"tmp/edges/iotdb_{i}/conf/iotdb-engine.properties",
                    lambda conf: modify_edge_config(conf, port))
        modify_file(f"tmp/edges/iotdb_{i}/conf/iotdb-env.sh",
                    lambda conf: modify_edge_env_config(conf))
        start_iotdb_server(f"tmp/edges/iotdb_{i}")
        # Starting sync server
        start_iotdb_server(f"tmp/edges/iotdb_{i}", "tools/start-sync-client.sh")
        # Start mass importing
        start_insert_threads(port)


    # We could start to find out when everything is synced
    print("Starting to wait till sync is finished!")

    finished_devices = 0
    while finished_devices < NUMBER_OF_EDGE_DEVICES * NUMBER_OF_WORKER_THREADS:
        print(f"{finished_devices} of {NUMBER_OF_EDGE_DEVICES} are synced...")
        try:
            session = Session("localhost", 6667, "root", "root")
            session.open(False)
            result: SessionDataSet = session.execute_query_statement("SELECT COUNT(*) FROM root.** align by device;")
            while result.has_next():
                record: RowRecord = result.next()
                device = record.get_fields()[0].get_string_value()
                count = record.get_fields()[1].get_long_value()
                # print(f"{device} : {count} ({count/(RECORDS_PER_EPOCH * EPOCHS) * 100}%)")
                if count == RECORDS_PER_EPOCH * EPOCHS:
                    finished_devices = finished_devices + 1
                    print(f" - Sync finished for device {device} - ")
            sleep(5)
            session.close()
        except:
            pass

    print("All devices are synced, stopping now!")

    before_end_hook()
