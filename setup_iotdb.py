import atexit
import os.path
import shutil
import subprocess
import threading
from os import path

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
        print(f"[{folder}] {line.decode('ascii')}", end="")


def start_iotdb_server(path, start_command="sbin/start-server.sh"):
    print(f"Starting IoTDB Server in folder {path}")
    t = threading.Thread(target=start_server, args=(path, start_command))
    t.setDaemon(False)
    t.start()


def before_end_hook():
    print("Initiating Stop Process")
    for p in processes_spawned:
        p: subprocess.Popen
        print(f"Killing process {p.pid}")
        p.kill()

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
    for i in range(1, 11):
        print(f"Creating Edge {i}")
        shutil.copytree("files/iotdb", f"tmp/edges/iotdb_{i}")
        modify_file(f"tmp/edges/iotdb_{i}/conf/iotdb-engine.properties", lambda conf: modify_edge_config(conf, 7000 + i))
        start_iotdb_server(f"tmp/edges/iotdb_{i}")
        # Starting sync server
        start_iotdb_server(f"tmp/edges/iotdb_{i}", "tools/start-sync-client.sh")
        # Start mass importing




    input("Press ENTER to stop")
    before_end_hook()








