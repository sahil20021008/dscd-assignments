from __future__ import print_function

import logging

from concurrent import futures

import grpc
import raft_pb2
import raft_pb2_grpc

nodes = {1: '34.70.82.58:50051', 2: '34.41.242.162:50051', 3: '34.70.180.161:50051',
        4: '34.30.136.0:50051', 5: '34.41.145.238:50051'}

if __name__ == '__main__':
    logging.basicConfig()
    id = int(input("Enter the ID of the leader: "))
    print("Which operation do you want to perform?")
    print("1. Get")
    print("2. Set")
    op = int(input("Enter the operation number: "))

    if op == 1:
        key = input("Enter the key: ")
        with grpc.insecure_channel(nodes[id]) as channel:
            stub = raft_pb2_grpc.ClientInteractsStub(channel)
            response = stub.GetPair(raft_pb2.getKey(key=key))
        if response.status == "Done":
            if response.data == "None":
                print("No such key found")
            else:
                print("The value of the key is: ", response.data)
        else:
            if response.leaderID == 0:
                print("No leader found")
            else:
                print("Possibly the leader of the cluster is: ", response.leaderID)

    elif op == 2:
        with grpc.insecure_channel(nodes[id]) as channel:
            stub = raft_pb2_grpc.ClientInteractsStub(channel)
            key = input("Enter the key: ")
            value = input("Enter the value: ")
            response = stub.SetPair(raft_pb2.Pair(key=key, value=value))
        if response.status == "Done":
            print("Pair added successfully")
        else:
            if response.leaderID == 0:
                print("No leader found")
            else:
                print("Possibly the leader of the cluster is: ", response.leaderID)
    else:
        print("Invalid operation")
        exit(0)
