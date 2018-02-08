import socket
import sys
import pandas as pd
import json
from time import sleep

def get_flow_data():
    df = pd.read_csv('/home/ds/data/netflow_d02tinyh.csv')
    response = df.to_json(orient='records')
    return response

def send_flows_to_spark(http_resp, tcp_connection):
    obj = json.loads(http_resp)
    for line in obj:
        try:
            payload = json.dumps(line)
            print(f"this line: {payload}")
            print(f"line length: {len(payload.encode())}")
            print(f"-------------------------")
            tcp_connection.send(payload.encode())
            sleep(1)
        except Exception as e:
            print(f"Error: {e}")

TCP_IP = socket.gethostbyname(socket.gethostname())
TCP_PORT = 9009
print(f"Host: {TCP_IP} :: Port {TCP_PORT}")

conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection....")
conn, addr = s.accept()
print("Connected... Sending flows")
try:
    resp = get_flow_data()
    send_flows_to_spark(resp, conn)
finally:
    conn.close()
