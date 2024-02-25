import zmq
import json
import socket
import uuid
import time
import threading
from datetime import datetime

uuid_str = str(uuid.uuid1())
print("Enter group name: ")
group_name = input()
group_ip = "34.171.114.10"
group_port = "5557"

context = zmq.Context()
server_socket = context.socket(zmq.REQ)
server_ip = "35.184.52.41"
server_port = "5555"
server_tcp = "tcp://" + server_ip + ":" + server_port
server_socket.connect(server_tcp)

user_socket = context.socket(zmq.ROUTER)
user_tcp = "tcp://*:" + group_port
user_socket.bind(user_tcp)

usertele = []
group_messages = {}


def server_function():
    temp_dict = {
        "name": group_name,
        "ip": group_ip,
        "port": group_port,
        "uuid": uuid_str,
    }
    message = json.dumps(temp_dict)
    server_socket.send_multipart([message.encode("utf-8")])
    message = server_socket.recv_multipart()[0]
    message = message.decode("utf-8")
    print(message)


def user_function():
    try:
        while True:
            identity, _, message = user_socket.recv_multipart()
            message = message.decode("utf-8")
            message = json.loads(message)
            if list(message.keys())[0] == "JOIN":
                usertele.append(message["JOIN"])
                print("JOIN REQUEST FROM " + str(message["JOIN"]))
                user_socket.send_multipart([identity, b"", b"SUCCESS"])
            elif list(message.keys())[0] == "LEAVE":
                try:
                    usertele.remove(message["LEAVE"])
                    print("LEAVE REQUEST FROM " + str(message["LEAVE"]))
                    user_socket.send_multipart([identity, b"", b"SUCCESS"])
                except ValueError:
                    print("There is no such user in this group")
                    user_socket.send_multipart([identity, b"", b"FAILURE"])
            elif list(message.keys())[0] == "GET":
                if message["GET"]["uuid"] in usertele:
                    timestamp = message["GET"]["timestamp"]
                    if timestamp == "":
                        message_dict = json.dumps(group_messages)
                        user_socket.send_multipart(
                            [identity, b"", message_dict.encode("utf-8")]
                        )
                    else:
                        message_dict = {}
                        for key, value in group_messages.items():
                            if key > timestamp:
                                message_dict[key] = value
                        message_dict = json.dumps(message_dict)
                        user_socket.send_multipart(
                            [identity, b"", message_dict.encode("utf-8")]
                        )
                    print("MESSAGE REQUEST FROM " + str(message["GET"]["uuid"]))
                else:
                    print("Invalid user " + str(message["GET"]["uuid"]))
                    user_socket.send_multipart([identity, b"", b"FAILURE"])
            elif list(message.keys())[0] == "SEND":
                if message["SEND"]["uuid"] in usertele:
                    timestamp = datetime.now()
                    timestamp = str(timestamp.strftime("%H:%M:%S"))
                    send_dict = {
                        "user": message["SEND"]["uuid"],
                        "message": message["SEND"]["message"],
                    }
                    group_messages[timestamp] = send_dict
                    print("MESSAGE SEND FROM " + str(message["SEND"]["uuid"]))
                    user_socket.send_multipart([identity, b"", b"SUCCESS"])
                else:
                    print("Invalid user " + str(message["SEND"]["uuid"]))
                    user_socket.send_multipart([identity, b"", b"FAILURE"])
            else:
                print("Invalid request")
                user_socket.send_multipart([identity, b"", b"INVALID REQUEST"])
    except KeyboardInterrupt:
        pass
    finally:
        user_socket.close()
        server_socket.close()
        context.term()


def main():
    server_thread = threading.Thread(target=server_function)
    user_thread = threading.Thread(target=user_function)

    server_thread.start()
    user_thread.start()

    server_thread.join()
    user_thread.join()


if __name__ == "__main__":
    main()
