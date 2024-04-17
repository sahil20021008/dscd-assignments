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


points = []

def getInputData():
    with open('Input/points3.txt') as f:
        for line in f:
            x, y = line.split(',')
            points.append((float(x), float(y)))

class MasterAndMapper(mapReduce_pb2_grpc.MasterAndMapperServicer):
    def mapRequest(self, request, context):
        startIdx = request.startIdx
        endIdx = request.endIdx
        iter_num = request.iter_num
        centroids = request.centroids
        reducers = request.reducers
        mapper_id = request.mapper_id

        print("Mapper received request for iteration %d" % (iter_num))
        print("Start index: %d, End index: %d" % (startIdx, endIdx))
        
        with open('Mappers/M%d/dump.txt' % (mapper_id), 'a') as f:
            f.write("Mapper received request for iteration %d\n" % (iter_num))
            f.write("Start index: %d, End index: %d\n" % (startIdx, endIdx))
            f.close()

        # print(centroids)
        # print(type(centroids))

        # for centroid in centroids:
        #     print("x: %.4f, y: %.4f" % (centroid.x, centroid.y))

        try:
            pairs = {}

            for i in range(startIdx, endIdx+1):
                min_dist = sys.maxsize
                min_idx = -1
                for j in range(1, len(centroids)+1):
                    dist = self.distance(mapReduce_pb2.Point(x=points[i][0], y=points[i][1]), centroids[j-1])
                    if dist < min_dist:
                        min_dist = dist
                        min_idx = j
                # print("Point (%.4f, %.4f) is closest to centroid %d" % (points[i][0], points[i][1], min_idx))
                if min_idx in pairs:
                    pairs[min_idx].append(mapReduce_pb2.Point(x=points[i][0], y=points[i][1]))
                else:
                    pairs[min_idx] = [mapReduce_pb2.Point(x=points[i][0], y=points[i][1])]

            self.partition(pairs, reducers, mapper_id, iter_num)

            with open('Mappers/M%d/dump.txt' % (mapper_id), 'a') as f:
                print("Writing pairs to dump.txt")
                f.write(pairs.__str__())
                f.write("\n")
                f.close()


            return mapReduce_pb2.status(state="SUCCESS")
        except Exception as e:
            return mapReduce_pb2.status(state="FAILURE")
    
    def partition(self, pairs, reducers, mapper_id, iteration):
        # create files R1.txt, R2.txt up to Rn.txt in Mappers/M1 folder
        for i in range(1, reducers+1):
            if os.path.exists("Mappers/M%d/R%d.txt" % (mapper_id, i)):
                with open("Mappers/M%d/R%d.txt" % (mapper_id, i), 'r') as file:
                    iter = file.readline()
                    # print(iter)
                    if int(iter) != iteration:
                        file = open("Mappers/M%d/R%d.txt" % (mapper_id, i), "w")
                        file.write(str(iteration))
                        file.write("\n")
                        file.close()
            else:
                file = open("Mappers/M%d/R%d.txt" % (mapper_id, i), "w")
                file.write(str(iteration))
                file.write("\n")
                file.close()
        
        # write the points to the files
        for key in pairs:
            file_num = key % reducers
            file = open("Mappers/M%d/R%d.txt" % (mapper_id, file_num+1), "a")
            # write the pair to the file with the key
            for pair in pairs[key]:
                file.write("%.4f,%.4f,%.4f\n" % (key, pair.x, pair.y))
            file.close()

    
    def distance(self, point1, point2):
        return ((point1.x - point2.x) ** 2 + (point1.y - point2.y) ** 2) ** 0.5
    
class MapperAndReducer(mapReduce_pb2_grpc.MapperAndReducerServicer):
    def getPairs(self, request, context):   
        reducer_id = request.reducer_id
        mapper_id = request.mapper_id

        # read from file Rn.txt
        file = open("Mappers/M%d/R%d.txt" % (mapper_id, reducer_id), "r")
        pairs = []
        skippedFirst = False
        for line in file:
            if not skippedFirst:
                skippedFirst = True
                continue
            centroid_id, x, y = line.split(',')
            pairs.append(mapReduce_pb2.Pair(centroid_id=float(centroid_id), point=mapReduce_pb2.Point(x=float(x), y=float(y))))
        file.close()

        return mapReduce_pb2.Pairs(pairs=pairs)
            



if __name__ == "__main__":
    getInputData()
    logging.basicConfig()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapReduce_pb2_grpc.add_MasterAndMapperServicer_to_server(MasterAndMapper(), server)
    mapReduce_pb2_grpc.add_MapperAndReducerServicer_to_server(MapperAndReducer(), server)
    server.add_insecure_port("[::]:50053")
    server.start()
    print("Mapper server started, listening on 50053")
    server.wait_for_termination()