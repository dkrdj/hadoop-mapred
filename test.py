import socket

hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)
print("ip :", ip_address)
