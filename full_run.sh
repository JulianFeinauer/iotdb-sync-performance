rm test.pcap
screen -d -S dump -m tcpdump -i lo -w test.pcap dst port 5555 or portrange 7001-7100
python3 setup_iotdb.py
screen -S dump -X quit
# Cleanup Sync Servers
ps aux | grep SyncClient | awk '{print $2}' | xargs kill
# Print tshark output
# tshark -Q -r test.pcap -z endpoints,tcp
