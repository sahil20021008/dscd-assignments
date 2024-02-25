from __future__ import print_function

import logging

from concurrent import futures

import grpc
import Client_Market_pb2
import Client_Market_pb2_grpc
import uuid
from threading import Thread

uniqueId = str(uuid.uuid1())

dict1 = {1: "Others", 2: "Electronics", 3: "Fashion", 4: "Any"}

def registerSeller():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        stub = Client_Market_pb2_grpc.SellerInteractsStub(channel)
        response = stub.RegisterSeller(Client_Market_pb2.SellerCredential(
            ipAddr="34.171.87.155",
            port=50052,
            uuid=uniqueId
        ))
    print(response.status)

def sellItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        print("Enter the item details")
        name = input("Name: ")
        price = float(input("Price: "))
        category_str = ""
        while(True):
            print("Choose the category: ")
            print("1. Electronics")
            print("2. Fashion")
            print("3. Others")
            category = int(input())
            if category == 1:
                category_str = "Electronics"
                break
            elif category == 2:
                category_str = "Fashion"
                break
            elif category == 3:
                category_str = "Others"
                break
            else:
                print("Invalid category")
        quantity = int(input("Quantity: "))
        description = input("Description: ") 
        stub = Client_Market_pb2_grpc.SellerInteractsStub(channel)
        cat = Client_Market_pb2.Category.Value(category_str)
        response = stub.SellItem(Client_Market_pb2.ItemRegistration(
            seller=Client_Market_pb2.SellerCredential(ipAddr = "34.171.87.155", port = 50052, uuid=uniqueId),
            name=name,
            price=price,
            category=cat,
            quantity=quantity,
            description=description
        ))  
    print(response.status)

def updateItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        print("Enter the item details")
        id = int(input("Item Id: "))
        newQuantity = int(input("New Quantity: "))
        newPrice = float(input("New Price: "))
        stub = Client_Market_pb2_grpc.SellerInteractsStub(channel)
        response = stub.UpdateItem(Client_Market_pb2.Item(
            seller=Client_Market_pb2.SellerCredential(ipAddr = "34.171.87.155", port = 50052, uuid=uniqueId),
            id=id,
            newQuantity=newQuantity,
            newPrice=newPrice
        ))  
    print(response.status)

def deleteItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        print("Enter the item details")
        id = int(input("Item Id: "))
        stub = Client_Market_pb2_grpc.SellerInteractsStub(channel)
        response = stub.DeleteItem(Client_Market_pb2.DelItem(
            seller=Client_Market_pb2.SellerCredential(ipAddr = "34.171.87.155", port = 50052, uuid=uniqueId),
            id=id
        ))  
    print(response.status)

def displayItems():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        stub = Client_Market_pb2_grpc.SellerInteractsStub(channel)
        response = stub.DisplaySellerItems(Client_Market_pb2.SellerCredential(ipAddr = "34.171.87.155", port = 50052, uuid=uniqueId))
    
    # print(response.__str__())
    for i in response.items:
        print("Item ID %d, Price %d, Name %s, Category %s" % (i.id, i.price, i.name, dict1[int(i.category)+1]))
        print("Description: %s" % i.description)
        print("Quantity: %d" % i.quantity)
        print("Rating: %.2f" % i.rating)
        print("Seller: %s:%d" % (i.seller.ipAddr, i.seller.port))
        print("----------------------------------------")


class MarketInteracts(Client_Market_pb2_grpc.MarketInteractsServicer):
    def NotifyClient(self, request, context):
        print("Item ID %d, Price %d, Name %s, Category %s" % (request.id, request.price, request.name, dict1[int(request.category)+1]))
        print("Description: %s" % request.description)
        print("Quantity: %d" % request.quantity)
        print("Rating: %.2f" % request.rating)
        print("Seller: %s:%d" % (request.seller.ipAddr, request.seller.port))
        return Client_Market_pb2.RequestStatus(status="SUCCESS")


def notification():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Client_Market_pb2_grpc.add_MarketInteractsServicer_to_server(MarketInteracts(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Seller server started, listening on 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    t = Thread(target=notification)
    t.start()
    logging.basicConfig()
    
    while(True):
        print("Please choose an option:" )
        print("1. Register the Seller")
        print("2. Sell Item")
        print("3. Update Item")
        print("4. Delete Item")
        print("5. Display Seller Items")
        print("6. Exit")
        option = int(input())
        if option == 1:
            registerSeller()
        elif option == 2:
            sellItem()
        elif option == 3:
            updateItem()
        elif option == 4:
            deleteItem()
        elif option == 5:
            displayItems()
        elif option == 6:
            break
        else:
            print("Invalid option")