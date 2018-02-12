import json
import socket

HOST, PORT = "localhost", 0

m = '{"id": 2, "name": "abc"}'
jsonObj = json.loads(m)


data = str(jsonObj)

print(type(data))
print(type(jsonObj))

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data
    sock.connect((HOST, PORT))
    sock.sendall(data)

    # Receive data from the server and shut down
    received = sock.recv(1024)
finally:
    sock.close()

print(f"Sent:     {data}")
print(f"Received: {received}")
