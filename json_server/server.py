# file:server.py
from jsonsocket import Server

host = 'LOCALHOST'
port = 32556

server = Server(host, port)

while True:
    server.accept()
    data = server.recv()
    server.send({"response": data})

server.close()
