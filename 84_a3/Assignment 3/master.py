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

centroids = []

mapper_ports = {1: 50052, 2: 50053, 3: 50054, 4: 50055, 5: 50056}

reducer_ports = {1: 50061, 2: 50062, 3: 50063, 4: 50064, 5: 50065}

threads = []

reducer_thread_run_again = []

def getInputData():
    with open('Input/points3.txt') as f:
        for line in f:
            x, y = line.split(',')
            points.append((float(x), float(y)))

def initializeCentroids(points, num_of_centroids):
    for i in range(num_of_centroids):
        # rand_idx = random.randint(0, len(points) - 1)
        # centroids.append(points[rand_idx])
        centroids.append(points[i])

def callMapper(start_index, end_index, iteration_num, mapper_id, reducers):
    mapper_addr = "localhost:" + str(mapper_ports[mapper_id])
    centroid_points = []

    for i in range(len(centroids)):
        centroid_points.append(mapReduce_pb2.Point(x=centroids[i][0], y=centroids[i][1]))

    # centroid_points = mapReduce_pb2.Points(points=centroid_points)

    map_data = mapReduce_pb2.mapData(startIdx=start_index, endIdx=end_index, iter_num=iteration_num, reducers=reducers,
                                     mapper_id = mapper_id, centroids=centroid_points)

    with grpc.insecure_channel(mapper_addr) as channel:
        try:
            stub = mapReduce_pb2_grpc.MasterAndMapperStub(channel)
            response = stub.mapRequest(map_data)
            if response.state == "SUCCESS":
                return True
            else:
                return False
        except Exception as e:
            return False


def callReducer(iteration_num, reducer_id, work_reducer, mappers):
    reducer_addr = "localhost:" + str(reducer_ports[reducer_id])

    # centroid_points = mapReduce_pb2.Points(points=centroid_points)

    reduce_data = mapReduce_pb2.reduceData(iter_num=iteration_num, my_reducer_id=reducer_id,
                                           work_reducer_id = work_reducer, mappers=mappers)

    done = False

    with grpc.insecure_channel(reducer_addr) as channel:
        try:
            stub = mapReduce_pb2_grpc.MasterAndReducerStub(channel)
            response = stub.reduceRequest(reduce_data)
            if response.state == "SUCCESS":
                done = True
        except Exception as e:
            done = False
    
    if not done:
        return [False, False]
    
    run_again = False
    for pair in response.pairs:
        # also check if any of the centroids have changed
        # if they have changed, then we need to run the algorithm again, but centroid values are float so use epsilon to be 0.01
        # print("Trying to access list index %d" % (int(pair.centroid_id - 1)))
        if abs(centroids[int(pair.centroid_id - 1)][0] - pair.point.x) > 0.001 or abs(centroids[int(pair.centroid_id - 1)][1] - pair.point.y) > 0.001:
            run_again = True
            print("reached here")
        # print("Centroid %d: (%.4f, %.4f)" % (pair.centroid_id, pair.point.x, pair.point.y))
        centroids[int(pair.centroid_id - 1)] = (pair.point.x, pair.point.y)

    return [run_again, True]
    

def call_parallel_mapper(start_index, end_index, iter, i, mappers, reducers):
    isDone = False
    assigned_mapper = i
    while not isDone:
        print("Task is assigned to Mapper %d, notifying it via gRPC" % (assigned_mapper))
        isDone = callMapper(start_index, end_index, iter, assigned_mapper, reducers)
        if not isDone:
            print("Mapper %d failed to complete the task assigned to it" % (assigned_mapper))
            assigned_mapper = (assigned_mapper % mappers) + 1
        else:
            print("Mapper %d completed the task assigned to it" % (assigned_mapper))

def call_parallel_reducer(iter, i, reducers, mappers):
    isDone = False
    assigned_reducer = i
    while not isDone:
        print("Task is assigned to Reducer %d, notifying it via gRPC" % (assigned_reducer))
        temp = callReducer(iter, assigned_reducer, i, mappers)
        isDone = temp[1]
        if not isDone:
            print("Reducer %d failed to complete the task assigned to it" % (assigned_reducer))
            assigned_reducer = (assigned_reducer % reducers) + 1
        else:
            print("Reducer %d completed the task assigned to it" % (assigned_reducer))
            reducer_thread_run_again.append(temp[0])
            return


if __name__ == '__main__':
    getInputData()
    # print(points)
    mappers = int(input('Enter the number of mappers: '))
    reducers = int(input('Enter the number of reducers: '))
    num_of_centroids = int(input('Enter the number of centroids: '))
    max_iterations = int(input('Enter the maximum number of iterations: '))
    initializeCentroids(points, num_of_centroids)
    print("Initial Centroids are: ", centroids)
    with open('dump_master.txt', 'w') as f:

        json.dump(centroids , f)

    num_of_points_per_mapper = len(points) // mappers

    for iter in range(1, max_iterations+1):
    
        print("Iteration %d" % (iter))

        start_index = 0
        end_index = num_of_points_per_mapper
        for i in range(1, mappers + 1):
            if i == mappers:
                end_index = len(points) - 1

            thread = Thread(target=call_parallel_mapper, args=(start_index, end_index, iter, i, mappers, reducers))
            threads.append(thread)
            thread.start()

            # isDone = False
            # assigned_mapper = i
            # while not isDone:
            #     print("Task is assigned to Mapper %d, notifying it via gRPC" % (assigned_mapper))
            #     isDone = callMapper(start_index, end_index, iter, assigned_mapper, reducers)
            #     if not isDone:
            #         print("Mapper %d failed to complete the task assigned to it" % (assigned_mapper))
            #         assigned_mapper = (assigned_mapper % mappers) + 1
            #     else:
            #         print("Mapper %d completed the task assigned to it" % (assigned_mapper))

            start_index = end_index + 1
            end_index = start_index + num_of_points_per_mapper

        # print("Initially threads are: ", threads)
        # print(len(threads))
        for thread in threads:
            thread.join()
        threads.clear()
        # print("finals threads are: ", threads)

    
        run_again = False
        for i in range(1, reducers + 1):

            thread = Thread(target=call_parallel_reducer, args=(iter, i, reducers, mappers))
            threads.append(thread)
            # print("size of threads: ", len(threads))
            # print("at iteration %d, reducer %d" % (iter, i))
            thread.start()

            # isDone = False
            # assigned_reducer = i
            # while not isDone:
            #     print("Task is assigned to Reducer %d, notifying it via gRPC" % (assigned_reducer))
            #     temp = callReducer(iter, assigned_reducer, i, mappers)
            #     isDone = temp[1]
            #     if not isDone:
            #         print("Reducer %d failed to complete the task assigned to it" % (assigned_reducer))
            #         assigned_reducer = (assigned_reducer % reducers) + 1
            #     else:
            #         print("Reducer %d completed the task assigned to it" % (assigned_reducer))
            #         run_again = run_again or temp[0]

        for thread in threads:
            thread.join()

        threads.clear()

        for status in reducer_thread_run_again:
            run_again = run_again or status

        reducer_thread_run_again.clear()

        if not run_again:
            print("Algorithm converged after %d iterations" % (iter))
            break

        print("Centroids after iteration %d: " % (iter), centroids)
        with open('dump_master.txt', 'a') as f:
            data_msg = "Centroids after iteration " + str(iter) + ": " + str(centroids)
            json.dump(data_msg, f)

    print("Final Centroids are: ", centroids)
    with open('dump_master.txt', 'a') as f:
        data_msg = "Final Centroids are: " + str(centroids)
        json.dump(data_msg, f)

    with open('centroids.txt', 'w') as f:
        for centroid in centroids:
            f.write("%.4f,%.4f\n" % (centroid[0], centroid[1]))
        # Waited for all mappers to finish
        


# For compilation: python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. mapReduce.proto