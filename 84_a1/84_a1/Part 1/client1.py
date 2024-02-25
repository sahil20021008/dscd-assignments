from __future__ import print_function

import logging

from concurrent import futures

import grpc
import Client_Market_pb2
import Client_Market_pb2_grpc
import uuid
from threading import Thread

dict1 = {1: "Others", 2: "Electronics", 3: "Fashion", 4: "Any"}

def searchItems():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        name = input("Enter the name of the item (Leave blank for all items): ")
        category_str = ""
        while(True):
            print("Choose the category: ")
            print("1. Electronics")
            print("2. Fashion")
            print("3. Others")
            print("4. ANY")
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
            elif category == 4:
                category_str = "Any"
                break
            else:
                print("Invalid category")
        cat = Client_Market_pb2.Category.Value(category_str)
        stub = Client_Market_pb2_grpc.BuyerInteractsStub(channel)
        response = stub.SearchItem(Client_Market_pb2.ItemSearch(
            buyer=Client_Market_pb2.BuyerCredential(ipAddr="34.133.59.31", port=50053),
            category=cat,
            name=name
        ))
    
    # print(response.__str__())
    for i in response.items:
        print("Item ID %d, Price %d, Name %s, Category %s" % (i.id, i.price, i.name, dict1[int(i.category)+1]))
        print("Description: %s" % i.description)
        print("Quantity: %d" % i.quantity)
        print("Rating: %.2f" % i.rating)
        print("Seller: %s:%d" % (i.seller.ipAddr, i.seller.port))
        print("----------------------------------------")

def buyItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        id = int(input("Enter the item id: "))
        quantity = int(input("Enter the quantity: "))
        stub = Client_Market_pb2_grpc.BuyerInteractsStub(channel)
        response = stub.BuyItem(Client_Market_pb2.ItemBuy(
            buyer=Client_Market_pb2.BuyerCredential(ipAddr="34.133.59.31", port=50053),
            id=id,
            quantity=quantity
        ))
    print(response.status)

def wishListItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        id = int(input("Enter the item id: "))
        stub = Client_Market_pb2_grpc.BuyerInteractsStub(channel)
        response = stub.AddToWishList(Client_Market_pb2.WishList(
            buyer=Client_Market_pb2.BuyerCredential(ipAddr="34.133.59.31", port=50053),
            id=id
        ))
    print(response.status)

def rateItem():
    with grpc.insecure_channel("34.69.190.134:50051") as channel:
        id = int(input("Enter the item id: "))
        while(True):
            print("Enter the rating (1-5): ")
            rating = int(input())
            if rating >= 1 and rating <= 5:
                break
            else:
                print("Invalid rating")
        stub = Client_Market_pb2_grpc.BuyerInteractsStub(channel)
        response = stub.RateItem(Client_Market_pb2.Rating(
            buyer=Client_Market_pb2.BuyerCredential(ipAddr="34.133.59.31", port=50053),
            id=id,
            rating=rating
        ))
    print(response.status)

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
    print("Buyer server started, listening on 50051")
    server.wait_for_termination()

if __name__ ==  "__main__":
    while(True):
        t = Thread(target=notification)
        t.start()
        logging.basicConfig()
        print("Please choose an option:")
        print("1. Search Items")
        print("2. Buy Item")
        print("3. Add Item to WishList")
        print("4. Rate Item")
        print("5. Exit")
        option = int(input())
        if option == 1:
            searchItems()
        elif option == 2:
            buyItem()
        elif option == 3:
            wishListItem()
        elif option == 4:
            rateItem()
        elif option == 5:
            break
        else:
            print("Invalid Option")