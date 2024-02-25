import zmq
import json
import socket
import uuid
import time
import threading

uuid_str = str(uuid.uuid1())

context = zmq.Context()
server_socket = context.socket(zmq.REQ)
server_ip = "35.184.52.41"
server_tcp = "tcp://" + server_ip + ":5556"
server_socket.connect(server_tcp)
group_dict = {}


def server_function():
    temp_dict = {"GET": uuid_str}
    temp_dict = json.dumps(temp_dict)
    server_socket.send_multipart([temp_dict.encode("utf-8")])
    message = server_socket.recv_multipart()[0]
    message = message.decode("utf-8")
    message = json.loads(message)
    global group_dict
    group_dict = message
    print("Group List: ")
    for key, value in message.items():
        print(value["name"] + " - " + value["ip"] + ":" + value["port"])


def joinGroup():
    print("Enter corresponding number for group you wish to join: ")
    for index, (key, value) in enumerate(group_dict.items()):
        print(f"{index}. {value['name']} - {value['ip']}:{value['port']}")
    choice = int(input())
    try:
        group = list(group_dict.values())[choice]
        group_socket = context.socket(zmq.REQ)
        group_tcp = "tcp://" + group["ip"] + ":" + group["port"]
        group_socket.connect(group_tcp)
        temp_dict = {"JOIN": uuid_str}
        temp_dict = json.dumps(temp_dict)
        group_socket.send_multipart([temp_dict.encode("utf-8")])
        message = group_socket.recv_multipart()[0]
        message = message.decode("utf-8")
        group_socket.close()
        if message == "SUCCESS":
            print("SUCCESS")
        else:
            print("FAILURE")
    except IndexError:
        print("Invalid choice")


def leaveGroup():
    print("Enter corresponding number for group you wish to leave: ")
    for index, (key, value) in enumerate(group_dict.items()):
        print(f"{index}. {value['name']} - {value['ip']}:{value['port']}")
    choice = int(input())
    try:
        group = list(group_dict.values())[choice]
        group_socket = context.socket(zmq.REQ)
        group_tcp = "tcp://" + group["ip"] + ":" + group["port"]
        group_socket.connect(group_tcp)
        temp_dict = {"LEAVE": uuid_str}
        temp_dict = json.dumps(temp_dict)
        group_socket.send_multipart([temp_dict.encode("utf-8")])
        message = group_socket.recv_multipart()[0]
        message = message.decode("utf-8")
        group_socket.close()
        if message == "SUCCESS":
            print("SUCCESS")
        else:
            print("FAILURE")
    except IndexError:
        print("Invalid choice")


def getMessage():
    print("Enter corresponding number for group you wish to get messages from: ")
    for index, (key, value) in enumerate(group_dict.items()):
        print(f"{index}. {value['name']} - {value['ip']}:{value['port']}")
    choice = int(input())
    print(
        "Please enter timestamp (HH:MM:SS format) of first message you want to get or leave empty if you want all messages: "
    )
    timestamp = input()
    try:
        group = list(group_dict.values())[choice]
        group_socket = context.socket(zmq.REQ)
        group_tcp = "tcp://" + group["ip"] + ":" + group["port"]
        group_socket.connect(group_tcp)
        temp_dict = {"GET": {"uuid": uuid_str, "timestamp": timestamp}}
        temp_dict = json.dumps(temp_dict)
        group_socket.send_multipart([temp_dict.encode("utf-8")])
        message = group_socket.recv_multipart()[0]
        message = message.decode("utf-8")
        if message == "FAILURE":
            print("FAILURE")
            return
        message = json.loads(message)
        for key, value in message.items():
            print(
                "TimeStamp: "
                + key
                + " User: "
                + value["user"]
                + " Message: "
                + value["message"]
            )
        group_socket.close()
    except IndexError:
        print("Invalid choice")


def sendMessage():
    print("Enter corresponding number for group you wish to send message to: ")
    for index, (key, value) in enumerate(group_dict.items()):
        print(f"{index}. {value['name']} - {value['ip']}:{value['port']}")
    choice = int(input())
    print("Please enter message: ")
    message = input()
    try:
        group = list(group_dict.values())[choice]
        group_socket = context.socket(zmq.REQ)
        group_tcp = "tcp://" + group["ip"] + ":" + group["port"]
        group_socket.connect(group_tcp)
        temp_dict = {"SEND": {"uuid": uuid_str, "message": message}}
        temp_dict = json.dumps(temp_dict)
        group_socket.send_multipart([temp_dict.encode("utf-8")])
        message = group_socket.recv_multipart()[0]
        message = message.decode("utf-8")
        group_socket.close()
        if message == "SUCCESS":
            print("SUCCESS")
        else:
            print("FAILURE")
    except IndexError:
        print("Invalid choice")


def group_function():
    print("Enter 1 to join a new group")
    print("Enter 2 to leave a group")
    print("Enter 3 to get messages from group")
    print("Enter 4 to send message to group")
    choice = input()
    if choice == "1":
        joinGroup()
    elif choice == "2":
        leaveGroup()
    elif choice == "3":
        getMessage()
    elif choice == "4":
        sendMessage()
    else:
        print("Invalid choice")


def main():
    while True:
        print("Enter 1 to send message to server")
        print("Enter 2 to communicate with groups")
        print("Enter 3 to exit")
        choice = input()
        if choice == "1":
            server_function()
        elif choice == "2":
            group_function()
        elif choice == "3":
            break
        else:
            print("Invalid choice")
    server_socket.close()
    context.term()


if __name__ == "__main__":
    main()
