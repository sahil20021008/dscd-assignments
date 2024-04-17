import random
import grpc
import mapReduce_pb2
import mapReduce_pb2_grpc
import queue
import logging
import sys
import os
import json
from concurrent import futures
from threading import Thread


mapper_ports = {1: 50052, 2: 50053, 3: 50054, 4: 50055, 5: 50056}


class MasterAndReducer(mapReduce_pb2_grpc.MasterAndReducerServicer):
    def reduceRequest(self, request, context):
        
        iter_num = request.iter_num
        my_reducer_id = request.my_reducer_id
        work_reducer_id = request.work_reducer_id
        mappers = request.mappers

        print("Reducer received request for iteration %d of tasks for reducer %d" % (iter_num, work_reducer_id))
        with open('Reducers/R%d/dump.txt' % (my_reducer_id), 'a') as f:
            f.write("Reducer received request for iteration %d\n" % (iter_num))
            f.close()

        try:
            pairs = self.shuffle_and_sort(my_reducer_id, mappers, work_reducer_id)
            # print("Pairs: ", pairs)
            new_centroids = self.reduce(pairs)

            with open('Reducers/R%d/dump.txt' % (my_reducer_id), 'a') as f:
                f.write("New centroids: \n")
                f.write(json.dumps(new_centroids))
                f.write("\n")

            # print("Pairs: ", pairs)
            # print("New centroids: ", new_centroids)

            ret_pairs = []
            for key in new_centroids:
                ret_pairs.append(mapReduce_pb2.Pair(centroid_id=float(key), point=mapReduce_pb2.Point(x=float(new_centroids[key][0]), y=float(new_centroids[key][1]))))

            return mapReduce_pb2.ReduceResp(pairs=ret_pairs, state="SUCCESS")
        except Exception as e:
            return mapReduce_pb2.ReduceResp(pairs=[], state="FAILURE")

        
    
    def shuffle_and_sort(self,reducer_id, mappers, work_reducer_id):
        print("Reducer ID %d is shuffling and sorting for reducer %d" % (reducer_id, work_reducer_id))
        pairs = {}
        for mapper in range(1,mappers+1):
            mapper_addr = "localhost:" + str(mapper_ports[mapper])
            with grpc.insecure_channel(mapper_addr) as channel:
                try:
                    stub = mapReduce_pb2_grpc.MapperAndReducerStub(channel)
                    response = stub.getPairs(mapReduce_pb2.Identity(reducer_id=work_reducer_id, mapper_id = mapper))
                    for pair in response.pairs:
                        if pair.centroid_id in pairs:
                            pairs[pair.centroid_id].append((pair.point.x, pair.point.y))
                        else:
                            pairs[pair.centroid_id] = [(pair.point.x, pair.point.y)]
                except Exception as e:
                    print("Error in connecting to mapper %d" % (mapper))

        return pairs
    
    def reduce(self, pairs):
        print("Reducer is reducing")
        new_centroids = {}
        for key in pairs:
            x_sum = 0
            y_sum = 0
            for point in pairs[key]:
                x_sum += point[0]
                y_sum += point[1]
            new_centroids[key] = (x_sum/len(pairs[key]), y_sum/len(pairs[key]))
        return new_centroids


if __name__ == "__main__":
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapReduce_pb2_grpc.add_MasterAndReducerServicer_to_server(MasterAndReducer(), server)
    server.add_insecure_port("[::]:50062")
    server.start()
    print("Reducer server started, listening on 50062")
    server.wait_for_termination()