# Implementing RAFT consensus algorithm, using grpc as the communication layer

import time
import random
import grpc
import raft_pb2
import raft_pb2_grpc
import queue
import logging
import sys
import os
import json
from concurrent import futures
from threading import Thread


class NonPersistentVariables:
    def __init__(self):
        self.timeout_time = None
        self.current_role = 'follower'
        self.current_leader = None
        self.votes_received = []
        self.sent_length = {}
        self.acked_length = {}
        self.lease_timer = 0    #time.time()
        self.my_lease = False


nodes = {1: '34.70.82.58:50051', 2: '34.41.242.162:50051', 3: '34.70.180.161:50051',
        4: '34.30.136.0:50051', 5: '34.41.145.238:50051'}

node_id = int(sys.argv[1])

if not os.path.exists('logs_node_%d' % node_id):
    os.makedirs('logs_node_%d' % node_id)


variables = NonPersistentVariables()

matadata_file_path = 'logs_node_%d/metadata.txt' % node_id
log_file_path = 'logs_node_%d/log.txt' % node_id
dump_file_path = 'logs_node_%d/dump.txt' % node_id

def reset_timeout():
    variables.timeout_time = time.time() + random.randint(5, 10)
    # print('Timeout reset to', variables.timeout_time)

def commitLogEntries():
    # print("called commit log entries for node: ", node_id)
    with open(matadata_file_path, 'r') as f:
        log = json.load(f)
        current_term = log['current_term']
        voted_for = log['voted_for']
        log_entries = log['log_entries']
        commit_length = log['commit_length']

    while commit_length < len(log_entries):
        acks_commit = 0
        for i in range(1, 6):
            if variables.acked_length[i] > commit_length:
                acks_commit += 1
        if acks_commit >= 3:
            commit_length += 1
            print('Node %d (leader) committed the entry SET %s %s to the state machine' % (node_id, log_entries[commit_length-1]['key'], log_entries[commit_length-1]['value']))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d (leader) committed the entry SET %s %s to the state machine\n' % (node_id, log_entries[commit_length-1]['key'], log_entries[commit_length-1]['value']))
        else:
            break   

    # print("New commit length is", commit_length)   

    with open(matadata_file_path, 'w') as f:
        json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)

def recv_log_response(response):
    follower = response.node_id
    term = response.term
    ack = response.ack
    success = response.status

    # print('Received log response from node %d with term %d, ack %d, success %s' % (follower, term, ack, success))

    with open(matadata_file_path, 'r') as f:
        log = json.load(f)
        current_term = log['current_term']
        voted_for = log['voted_for']
        log_entries = log['log_entries']
        commit_length = log['commit_length']

    if term > current_term:
        # print("Follower sent a response with a higher term. Stepping down")
        reset_timeout()
        current_term = term
        voted_for = []
        variables.current_role = 'follower'
        variables.current_leader = None
        variables.votes_received = []
        with open(matadata_file_path, 'w') as f:
            json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
        print('Node %d Stepping down' % node_id)
        with open(dump_file_path, 'a') as f:
            f.write('Node %d Stepping down\n' % node_id)

    elif term == current_term and variables.current_role == 'leader':
        # print('Received log response from node %d with ack %d' % (follower, ack))
        if success == 'True' and ack >= variables.acked_length[follower]:
            variables.sent_length[follower] = ack
            variables.acked_length[follower] = ack
            commitLogEntries()
        elif variables.sent_length[follower] > 0:
            # print('Node %d (leader) retrying to send log entries to node %d' % (node_id, follower))
            variables.sent_length[follower] -= 1
            replicateLog(node_id, follower, float(time.time() + 10))

        # with open(matadata_file_path, 'w') as f:
        #     json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)

def replicateLog(src, dest, lease_time, no_op=False):

    # print('Replicating log from %d to %d' % (src, dest))
    # print("In replicateLog function ", inspect.stack()[1].function)
    with open(matadata_file_path, 'r') as f:
        log = json.load(f)
        current_term = log['current_term']
        voted_for = log['voted_for']
        log_entries = log['log_entries']
        commit_length = log['commit_length']
    
    prefixLen = variables.sent_length[dest]
    suffix = []
    for i in range(prefixLen, len(log_entries)):
        suffix.append(
            raft_pb2.LogEntry(
                key = log_entries[i]['key'],
                value = log_entries[i]['value'],
                term = int(log_entries[i]['term'])
            )
        )
    prefixTerm = 0
    if prefixLen > 0:
        prefixTerm = log_entries[prefixLen-1]['term']
    
    with grpc.insecure_channel(nodes[dest]) as channel:
        stub = raft_pb2_grpc.LeaderInteractsStub(channel)
        
        try:
            # print("Sending lease time of ", lease_time, "to node ", dest)
            response = stub.LogRequest(raft_pb2.LeaderDetails(
                leaderID=src,
                term=current_term,
                prefixLen=prefixLen,
                prefixTerm=prefixTerm,
                leaderCommit=commit_length,
                suffix = suffix,
                no_op = str(no_op),
                leaseTime = lease_time
            ))
            recv_log_response(response)
            # print('Was successfully able to send RPC to node %d' % dest)
            return 1
        except Exception as e:
            # print('Node %d is down' % dest)
            # print("Exception is: ", repr(e)) # sahil commented
            # print("Exception details are: ", e.details()) # sahil commented
            print('Error occurred while sending RPC to node %d' % dest)
            with open(dump_file_path, 'a') as f:
                f.write('Error occurred while sending RPC to node %d\n' % dest)
            return 0

def appendEntries(prefixLen, leaderCommit, suffix):

    # print('Appending entries')
    # print("In append entries function with prefixLen: ", prefixLen, " leaderCommit: ", leaderCommit, " suffix: ", suffix)

    with open(matadata_file_path, 'r') as f:
        log = json.load(f)
        current_term = log['current_term']
        voted_for = log['voted_for']
        log_entries = log['log_entries']
        commit_length = log['commit_length']
    
    if len(suffix) > 0 and len(log_entries) > prefixLen:
        index = min(len(log_entries), prefixLen + len(suffix)) - 1
        if log_entries[index]['term'] != suffix[index - prefixLen].term:
            log_entries = log_entries[:prefixLen]

    if prefixLen + len(suffix) > len(log_entries):
        for i in range(len(log_entries) - prefixLen, len(suffix)):
            log_entries.append({'term': suffix[i].term, 'key': suffix[i].key, 'value': suffix[i].value})
    
    with open(log_file_path, 'w') as f:
        for entry in log_entries:
            if entry['key'] == 'NO_OP':
                f.write('NO_OP %d\n' % entry['term'])
            else:
                f.write('SET %s %s %d\n' % (entry['key'], entry['value'], entry['term']))
    
    if leaderCommit > commit_length:
        # print('Leader commit length is', leaderCommit, 'current commit length is', commit_length, 'for node', node_id
        #       , 'and leader is', variables.current_leader)
        for i in range(commit_length, leaderCommit):
            print('Node %d (follower) committed the entry SET %s %s to the state machine' % (node_id, log_entries[i]['key'], log_entries[i]['value']))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d (follower) committed the entry SET %s %s to the state machine\n' % (node_id, log_entries[i]['key'], log_entries[i]['value']))
        commit_length = leaderCommit
    
    with open(matadata_file_path, 'w') as f:
        json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
        

def recv_vote_response(response):

    # print('Received vote response from node %d and term %d with vote %s' % (response.node_id, response.term, response.granted))

    with open(matadata_file_path, 'r') as f:
        log = json.load(f)
        current_term = log['current_term']
        voted_for = log['voted_for']
        log_entries = log['log_entries']
        commit_length = log['commit_length']

    voterID = response.node_id
    term = response.term
    vote_granted = response.granted
    if response.leaseTime > variables.lease_timer:
        variables.lease_timer = response.leaseTime

    if variables.current_role == 'candidate' and term == current_term and vote_granted == 'True' and voterID not in variables.votes_received:
        variables.votes_received.append(voterID)
        if len(variables.votes_received) >= 3:
            variables.current_role = 'leader'
            variables.current_leader = node_id
            reset_timeout()
            # print('Node %d became the leader for term %d' % (node_id, current_term))
            # with open(dump_file_path, 'a') as f:
            #     f.write('Node %d became the leader for term %d\n' % (node_id, current_term))
            # print('node %d is the leader' % node_id)

            printed_once = False
            # print("In recv_vote_response. Current time is ", time.time(), "lease time is", variables.lease_timer)
            while time.time() < variables.lease_timer:
                if not printed_once:
                    print('New Leader %d waiting for Old Leader Lease to timeout ' % node_id)
                    with open(dump_file_path, 'a') as f:
                        f.write('New Leader %d waiting for Old Leader Lease to timeout\n' % node_id)
                    printed_once = True
                time.sleep(0.1)

            print('Node %d became the leader for term %d' % (node_id, current_term))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d became the leader for term %d\n' % (node_id, current_term))

            no_op_key = 'NO_OP'
            no_op_value = 'NO_OP'
            log_entries.append({'term': current_term, 'key': no_op_key, 'value': no_op_value})
            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
            variables.acked_length[node_id] = len(log_entries)

            
            variables.my_lease = True
            # start_time = time.time()
            # no_op_acks = 0
            for i in range(1, 6):
                if i != node_id:
                    variables.sent_length[i] = len(log_entries) - 1
                    variables.acked_length[i] = 0
                    # no_op_acks += replicateLog(node_id, i, float(start_time+10), True)
            # if no_op_acks >= 2:
            #     variables.lease_timer = start_time + 10
            # print("Now I have the lease with lease time: ", variables.lease_timer)


    elif term > current_term:
        # print("Got a vote response with a higher term. Stepping down")
        reset_timeout()
        variables.current_role = 'follower'
        variables.current_leader = None
        variables.votes_received = []
        current_term = term
        voted_for = []
        with open(matadata_file_path, 'w') as f:
            json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
        print('Node %d Stepping down' % node_id)
        with open(dump_file_path, 'a') as f:
            f.write('Node %d Stepping down\n' % node_id)


# This method will run continuously in the background and will be responsible for sending vote requests and election timer
def election_timer_and_vote_request():
    while(True):

        with open(matadata_file_path, 'r') as f:
            try:
                log = json.load(f)
                current_term = log['current_term']
                voted_for = log['voted_for']
                log_entries = log['log_entries']
                commit_length = log['commit_length']
            except Exception as e:
                time.sleep(0.1)
                continue

        # print('current time is', time.time(), 'timeout time is', variables.timeout_time, 'current role is', variables.current_role, 'current term is', current_term, 'voted for', voted_for, 'log entries', log_entries, 'commit length', commit_length, 'votes received', variables.votes_received, 'sent length', variables.sent_length, 'acked length', variables.acked_length)
        # print("Current time is", time.time(), "timeout time is", variables.timeout_time, 'lease time is', variables.lease_timer)

        if time.time() > variables.timeout_time and (variables.current_role == 'follower' or variables.current_role == 'candidate'):
            print('Node %d election timer timed out, starting election' % node_id)
            with open(dump_file_path, 'a') as f:
                f.write('Node %d election timer timed out, starting election\n' % node_id)
            reset_timeout()
            # print('new timeout is', variables.timeout_time)
            variables.current_role = 'candidate'
            current_term += 1
            voted_for = [node_id]
            variables.votes_received = [node_id]
            variables.my_lease = False
            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
            last_term = 0
            if len(log_entries) > 0:
                last_term = log_entries[len(log_entries)-1]['term']
            
            for i in range(1, 6):
                if i != node_id:
                    with grpc.insecure_channel(nodes[i]) as channel:
                        stub = raft_pb2_grpc.CandidateInteractsStub(channel)

                        try:
                            response = stub.VoteRequest(raft_pb2.Details(
                                node_id=node_id,
                                current_term=current_term,
                                log_length=len(log_entries),
                                last_term=last_term
                            ))
                            recv_vote_response(response)
                        except Exception as e:
                            print('Node %d is down' % i)
                            # print('Error occurred while sending RPC to node %d' % i)
                            pass  
                
        time.sleep(0.1)

def heartbeat():
    while(True):
        # if variables.current_role == 'leader' and (variables.my_lease == True or (variables.my_lease == False and time.time() > variables.lease_timer)):
        if variables.current_role == 'leader' and variables.my_lease == True:
            # print('Sending heartbeat')
            print('Leader %d sending heartbeat and renewing lease' % node_id)
            with open(dump_file_path, 'a') as f:
                f.write('Leader %d sending heartbeat and renewing lease\n' % node_id)

            with open(matadata_file_path, 'r') as f:
                log = json.load(f)
                current_term = log['current_term']
                voted_for = log['voted_for']
                log_entries = log['log_entries']
                commit_length = log['commit_length']
            # print('current role is', variables.current_role, 'current term is', current_term, 'voted for', voted_for, 'log entries', log_entries, 'commit length', commit_length, 'votes received', variables.votes_received, 'sent length', variables.sent_length, 'acked length', variables.acked_length)
            # variables.my_lease = True
            heartbeat_acks = 0
            start_time = time.time()
            for i in range(1, 6):
                if i != node_id:
                    heartbeat_acks +=  replicateLog(node_id, i, float(start_time+10))
            if heartbeat_acks >= 2:
                variables.lease_timer = start_time + 5
                # print("In heartbeat, current time is ", time.time(), "lease time is", variables.lease_timer)
            elif time.time() > variables.lease_timer:
                print('Leader %d lease renewal failed. Stepping down. ' % node_id)
                with open(dump_file_path, 'a') as f:
                    f.write('Leader %d lease renewal failed. Stepping down.\n' % node_id)

                reset_timeout()
                variables.current_role = 'follower'
                variables.current_leader = None
                variables.votes_received = []
                voted_for = []
                with open(matadata_file_path, 'w') as f:
                    json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
                variables.my_lease = False
            # do same wherever you are sending the log request
        time.sleep(1)

class CandidateInteracts(raft_pb2_grpc.CandidateInteractsServicer):
    def VoteRequest(self, request, context):
        cID = request.node_id
        cterm = request.current_term
        clog_length = request.log_length
        clast_term = request.last_term

        with open(matadata_file_path, 'r') as f:
            log = json.load(f)
            current_term = log['current_term']
            voted_for = log['voted_for']
            log_entries = log['log_entries']
            commit_length = log['commit_length']

        if cterm > current_term:
            # print('received a vote request with higher term from node %d' % cID)
            reset_timeout()
            current_term = cterm
            variables.current_role = 'follower'
            variables.current_leader = None
            variables.votes_received = []
            voted_for = []

            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)

        # print('Received vote request from node %d with term %d, log length %d, last term %d' % (cID, cterm, clog_length, clast_term))

        last_term = 0
        if len(log_entries) > 0:
            last_term = log_entries[len(log_entries)-1]['term']

        logOK = (clast_term > last_term) or (clast_term == last_term and clog_length >= len(log_entries))

        if (cterm == current_term) and (voted_for == [] or voted_for[0] == cID) and logOK:
            voted_for = [cID]
            reset_timeout()
            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
            print('Vote Granted for node %d in term %d' % (cID, cterm))
            with open(dump_file_path, 'a') as f:
                f.write('Vote Granted for node %d in term %d\n' % (cID, cterm))
            return raft_pb2.VoteResponse(node_id=node_id, term=current_term, granted='True', leaseTime= float(variables.lease_timer))
        else:
            print('Vote Denied for node %d in term %d' % (cID, cterm))
            with open(dump_file_path, 'a') as f:
                f.write('Vote Denied for node %d in term %d\n' % (cID, cterm))
            return raft_pb2.VoteResponse(node_id=node_id, term=current_term, granted='False', leaseTime= float(variables.lease_timer))
        
class ClientInteracts(raft_pb2_grpc.ClientInteractsServicer):
    def SetPair(self, request, context):
        key = request.key
        value = request.value

        # print('Received set request for key %s and value %s' % (key, value))

        if variables.current_role == 'leader':
            print('Node %d (leader) received an entry SET %s %s' % (node_id, key, value))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d (leader) received an entry SET %s %s\n' % (node_id, key, value))
            if variables.my_lease == False:
                print('Leader %d does not have the lease.' % node_id)
        
        if variables.current_role == 'leader' and variables.my_lease == True:
            with open(matadata_file_path, 'r') as f:
                log = json.load(f)
                current_term = log['current_term']
                voted_for = log['voted_for']
                log_entries = log['log_entries']
                commit_length = log['commit_length']

            log_entries.append({'term': current_term, 'key': key, 'value': value})
            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
            variables.acked_length[node_id] = len(log_entries)
            successfully_replicated = 1

            start_time = time.time()
            for i in range(1, 6):
                if i != node_id:
                    successfully_replicated += replicateLog(node_id, i, float(start_time+10))
            # This part needs to be modified later for get and also to send success only when the log entry is committed
            if successfully_replicated >= 3:
                variables.lease_timer = start_time + 5
                return raft_pb2.ServerResponse(data = "None", leaderID = node_id, status = "Done")
            else:
                return raft_pb2.ServerResponse(data = "None", leaderID = variables.current_leader, status = "Fail")
        else:
            return raft_pb2.ServerResponse(data = "None", leaderID = variables.current_leader, status = "Fail")
        
    def GetPair(self, request, context):
        key = request.key

        # print('Received get request for key %s' % key)

        with open(matadata_file_path, 'r') as f:
            log = json.load(f)
            current_term = log['current_term']
            voted_for = log['voted_for']
            log_entries = log['log_entries']
            commit_length = log['commit_length']

        if variables.current_role == 'leader':
            print('Node %d (leader) received an entry GET %s' % (node_id, key))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d (leader) received an entry GET %s\n' % (node_id, key))
            if variables.my_lease == False:
                print('Leader %d does not have the lease.' % node_id)

        if variables.current_role == 'leader' and variables.my_lease == True:
            for i in range(len(log_entries)-1, -1, -1):
                if log_entries[i]['key'] == key:
                    return raft_pb2.ServerResponse(data = log_entries[i]['value'], leaderID = node_id, status = "Done")
            return raft_pb2.ServerResponse(data = "None", leaderID = node_id, status = "Done")
        else:
            return raft_pb2.ServerResponse(data = "None", leaderID = variables.current_leader, status = "Fail")

class LeaderInteracts(raft_pb2_grpc.LeaderInteractsServicer):
    def LogRequest(self, request, context):
        leaderID = request.leaderID
        term = request.term
        prefixLen = request.prefixLen
        prefixTerm = request.prefixTerm
        leaderCommit = request.leaderCommit
        suffix = request.suffix
        # no_op = request.no_op
        leaseTime = request.leaseTime

        # print("Got a lease time of ", request.leaseTime)

        # print('Received log request from leader %d with term %d, prefix length %d, prefix term %d, leader commit %d, suffix length %d' % (leaderID, term, prefixLen, prefixTerm, leaderCommit, len(suffix)))
        # print('no_op is', no_op, 'lease time is', leaseTime)

        with open(matadata_file_path, 'r') as f:
            log = json.load(f)
            current_term = log['current_term']
            voted_for = log['voted_for']
            log_entries = log['log_entries']
            commit_length = log['commit_length']

        if term > current_term:
            reset_timeout()
            current_term = term
            voted_for = []
            variables.votes_received = []
            with open(matadata_file_path, 'w') as f:
                json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)
        
        if term == current_term:
            reset_timeout()
            variables.current_role = 'follower'
            variables.current_leader = leaderID
            variables.my_lease = False
            variables.lease_timer = leaseTime

        # print("Received logRequest. Now timeout_timer is ", variables.timeout_time, "lease time is ", variables.lease_timer)


        logOK = (len(log_entries) >= prefixLen) and (prefixLen == 0 or log_entries[prefixLen-1]['term'] == prefixTerm)

        if term == current_term and logOK:
            print('Node %d accepted AppendEntries RPC from leader %d' % (node_id, leaderID))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d accepted AppendEntries RPC from leader %d\n' % (node_id, leaderID))
            appendEntries(prefixLen, leaderCommit, suffix)
            ack = prefixLen + len(suffix)
            return raft_pb2.LogResponse(node_id=node_id, term=current_term, ack=ack, status='True')
        else:
            print('Node %d rejected AppendEntries RPC from leader %d' % (node_id, leaderID))
            with open(dump_file_path, 'a') as f:
                f.write('Node %d rejected AppendEntries RPC from leader %d\n' % (node_id, leaderID))
            return raft_pb2.LogResponse(node_id=node_id, term=current_term, ack=0, status='False')


if __name__ == '__main__':
    # check if being recovered from a crash or a clean start, if it is a crash, recover the state from the log file and continue otherwise create a new log file
    # path of matadata file is /logs_node_{node_id}/matadata.txt
    if os.path.exists(matadata_file_path):
        with open(matadata_file_path, 'r') as f:
            log = json.load(f)
            current_term = log['current_term']
            voted_for = log['voted_for']
            log_entries = log['log_entries']
            commit_length = log['commit_length']
    else:
        current_term = 0
        voted_for = []
        log_entries = []
        commit_length = 0
        # create a new log file to store the state
        with open(matadata_file_path, 'w') as f:
            json.dump({'current_term': current_term, 'voted_for': voted_for, 'log_entries': log_entries, 'commit_length': commit_length}, f)

    reset_timeout()
    t = Thread(target=election_timer_and_vote_request)
    t.start()
    t2 = Thread(target=heartbeat)
    t2.start()
    logging.basicConfig()  
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_CandidateInteractsServicer_to_server(CandidateInteracts(), server)
    raft_pb2_grpc.add_ClientInteractsServicer_to_server(ClientInteracts(), server)
    raft_pb2_grpc.add_LeaderInteractsServicer_to_server(LeaderInteracts(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

    
    


