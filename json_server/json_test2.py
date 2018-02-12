import json
import socket

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

host = socket.gethostname()
port = 5000

buffer_size = 4096

serverSocket.bind((host, port))
serverSocket.listen(10)

print("Listening on %s:%s..." % (host, str(port)))

while True:
    clientSocket, address = serverSocket.accept()
    data = clientSocket.recv(buffer_size)
    jdata = json.loads(data.decode('utf-8'))

    print("Connection received from %s..." % str(address))
    clientSocket.send(jdata)
    clientSocket.close()
