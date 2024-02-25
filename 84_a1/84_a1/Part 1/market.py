from concurrent import futures
import logging

import grpc

import Client_Market_pb2
import Client_Market_pb2_grpc

sellers = {}
items = {}
wishlist = {}
hasRated = {}

class SellerInteracts(Client_Market_pb2_grpc.SellerInteractsServicer):
    def RegisterSeller(self, request, context):
        ipAddr = request.ipAddr
        port = request.port
        sellerId = request.uuid

        print("Seller join request from %s:%d, uuid=%s" % (ipAddr, port, sellerId))

        if sellerId in sellers:
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            sellers[sellerId] = (ipAddr, port)
            return Client_Market_pb2.RequestStatus(status="SUCCESS")
        
    def SellItem(self, request, context):
        seller = request.seller
        name = request.name
        price = request.price
        category = request.category
        quantity = request.quantity
        description = request.description

        if seller.uuid not in sellers:
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            print("Sell Item request from %s:%d " % (sellers[seller.uuid][0], sellers[seller.uuid][1]))
            idx =  1
            if len(items) > 0:
                idx = max(items.keys()) + 1
            rating = 0.0
            reviews = 0
            items[idx] = (seller.uuid, name, price, category, quantity, description, rating, reviews)
            return Client_Market_pb2.RequestStatus(status="SUCCESS %d" % idx)
        
    def UpdateItem(self, request, context):
        seller = request.seller
        id = request.id
        newQuantity = request.newQuantity
        newPrice = request.newPrice

        if (seller.uuid not in sellers) or (id not in items) or (items[id][0] != seller.uuid):
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            print("Update Item %d request from %s:%d " % (id, sellers[seller.uuid][0], sellers[seller.uuid][1]))
            items[id] = (items[id][0], items[id][1], newPrice, items[id][3], newQuantity, items[id][5], items[id][6], items[id][7])
            if id in wishlist:
                for buyer in wishlist[id]:
                    with grpc.insecure_channel(buyer[0] + ":" + "50051") as channel:
                        stub = Client_Market_pb2_grpc.MarketInteractsStub(channel)
                        stub.NotifyClient(Client_Market_pb2.ItemReceived(
                            id=id,
                            name=items[id][1],
                            price=items[id][2],
                            category=items[id][3],
                            quantity=items[id][4],
                            description=items[id][5],
                            rating=items[id][6],
                            seller = Client_Market_pb2.SellerDetails(ipAddr=sellers[items[id][0]][0], port=sellers[items[id][0]][1])
                        ))
            return Client_Market_pb2.RequestStatus(status="SUCCESS")
        
    def DeleteItem(self, request, context):
        seller = request.seller
        id = request.id

        if (seller.uuid not in sellers) or (id not in items) or (items[id][0] != seller.uuid):
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            print("Delete Item %d request from %s:%d " % (id, sellers[seller.uuid][0], sellers[seller.uuid][1]))
            del items[id]
            return Client_Market_pb2.RequestStatus(status="SUCCESS")
        
    def DisplaySellerItems(self, request, context):
        uniqueId = request.uuid
        ipAddr = request.ipAddr
        port = request.port
        itemsRet = []

        if uniqueId not in sellers:
            return Client_Market_pb2.Items(items=itemsRet)
        else:
            print("Display Items request from %s:%d " % (sellers[uniqueId][0], sellers[uniqueId][1]))
            for id in items:
                if items[id][0] == uniqueId:
                    itemsRet.append(Client_Market_pb2.ItemReceived(
                        id=id,
                        name=items[id][1],
                        price=items[id][2],
                        category=items[id][3],
                        quantity=items[id][4],
                        description=items[id][5],
                        rating=items[id][6],
                        seller = Client_Market_pb2.SellerDetails(ipAddr=sellers[uniqueId][0], port=sellers[uniqueId][1])
                    ))        
            return Client_Market_pb2.Items(items=itemsRet)
        
class BuyerInteracts(Client_Market_pb2_grpc.BuyerInteractsServicer):
    def SearchItem(self, request, context):
        buyer = request.buyer
        category = request.category
        name = request.name
        itemsRet = []

        print("Search request for item name: %s, category: %s" % (name, category))

        for id in items:
            if (category == 3 or items[id][3] == category) and (name == "" or items[id][1] == name):
                itemsRet.append(Client_Market_pb2.ItemReceived(
                    id=id,
                    name=items[id][1],
                    price=items[id][2],
                    category=items[id][3],
                    quantity=items[id][4],
                    description=items[id][5],
                    rating=items[id][6],
                    seller = Client_Market_pb2.SellerDetails(ipAddr=sellers[items[id][0]][0], port=sellers[items[id][0]][1])
                ))
        return Client_Market_pb2.Items(items=itemsRet)
    
    def BuyItem(self, request, context):
        buyer = request.buyer
        id = request.id
        quantity = request.quantity

        print("Buy request %d of Item %d from %s:%d " % (quantity, id, buyer.ipAddr, buyer.port))

        if id not in items:
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            if items[id][4] < quantity:
                return Client_Market_pb2.RequestStatus(status="FAIL")
            else:
                items[id] = (items[id][0], items[id][1], items[id][2], items[id][3], items[id][4] - quantity, items[id][5], items[id][6], items[id][7])
                notify_seller = sellers[items[id][0]]
                with grpc.insecure_channel(notify_seller[0] + ":" + "50051") as channel:
                    stub = Client_Market_pb2_grpc.MarketInteractsStub(channel)
                    stub.NotifyClient(Client_Market_pb2.ItemReceived(
                        id=id,
                        name=items[id][1],
                        price=items[id][2],
                        category=items[id][3],
                        quantity=items[id][4],
                        description=items[id][5],
                        rating=items[id][6],
                        seller = Client_Market_pb2.SellerDetails(ipAddr=notify_seller[0], port=notify_seller[1])
                    ))
                return Client_Market_pb2.RequestStatus(status="SUCCESS")
            
    def AddToWishList(self, request, context):
        buyer = request.buyer
        id = request.id

        print("Add to wishlist request of Item %d from %s:%d " % (id, buyer.ipAddr, buyer.port))

        if (id not in items) or (id in wishlist and (buyer.ipAddr, buyer.port) in wishlist[id]):
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            if id not in wishlist:
                wishlist[id] = []
            wishlist[id].append((buyer.ipAddr, buyer.port))
            return Client_Market_pb2.RequestStatus(status="SUCCESS")
        
    def RateItem(self, request, context):
        buyer = request.buyer
        id = request.id
        rating = request.rating

        print("%s:%d rated Item %d with %d" % (buyer.ipAddr, buyer.port, id, rating))

        if (id not in items) or ((id in hasRated) and (buyer.ipAddr, buyer.port) in hasRated[id]):
            return Client_Market_pb2.RequestStatus(status="FAIL")
        else:
            if id not in hasRated:
                hasRated[id] = []
            hasRated[id].append((buyer.ipAddr, buyer.port))
            items[id] = (items[id][0], items[id][1], items[id][2], items[id][3], 
                         items[id][4], items[id][5], (items[id][6] + rating)/(items[id][7] + 1), items[id][7] + 1)
            return Client_Market_pb2.RequestStatus(status="SUCCESS")
        

if __name__ == "__main__":
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Client_Market_pb2_grpc.add_SellerInteractsServicer_to_server(SellerInteracts(), server)
    Client_Market_pb2_grpc.add_BuyerInteractsServicer_to_server(BuyerInteracts(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Market server started, listening on 50051")
    server.wait_for_termination()
