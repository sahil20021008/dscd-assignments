import zmq
import time
import socket
import uuid
import json
import threading

group_dict = {}

context = zmq.Context()
group = context.socket(zmq.ROUTER)
group.bind("tcp://*:5555")

user = context.socket(zmq.ROUTER)
user.bind("tcp://*:5556")

poller = zmq.Poller()
poller.register(group, zmq.POLLIN)
poller.register(user, zmq.POLLIN)


def messageServer():
    identity, _, message = group.recv_multipart()
    message = message.decode("utf-8")
    message = json.loads(message)
    group_dict[message["uuid"]] = message
    print(
        "JOIN REQUEST FROM "
        + str(message["ip"])
        + ":"
        + str(message["port"])
        + " NAME: "
        + str(message["name"])
        + " UUID: "
        + str(message["uuid"])
    )
    group.send_multipart([identity, b"", b"SUCCESS"])


def getGroupList():
    identity, _, message = user.recv_multipart()
    message = message.decode("utf-8")
    message = json.loads(message)
    group_list = json.dumps(group_dict)
    if list(message.keys())[0] == "GET":
        print("GROUP LIST REQUEST FROM " + str(list(message.values())[0]))
        user.send_multipart([identity, b"", group_list.encode("utf-8")])
    else:
        print("Invalid request from " + str(list(message.values())[0]))
        user.send_multipart([identity, b"", b"INVALID REQUEST"])


while True:
    try:
        checker = dict(poller.poll())
    except KeyboardInterrupt:
        break
    if group in checker:
        server_thread = threading.Thread(target=messageServer)
        server_thread.start()
    if user in checker:
        user_thread = threading.Thread(target=getGroupList)
        user_thread.start()

group.close()
user.close()
context.term()
