import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
import random
import grpc
import sys

from threading import Timer, Thread
from concurrent import futures
from enum import Enum

config_file = 'config.conf'
servers = {} # Dictionary of servers -> { id: address, ...}
servers_log = {} # Dictinary of servers -> { id: (nextIndex, matchIndex), ...}

STATE = None # Server state: Follower, Candidate, Leader
TERM_NUMBER = 0 # Current term
LEADER = None # Current leader in the system
SERVER_ID = None # Own server id (0, 1, 2, 3, 4, ...)
SERVER_ADDRESS = None # Own server address
VOTED = None # Id of the server that was voted for 
TIMEOUT = None # Random interval for timers

TIMER_THREAD = None # Thread for general timers: election, heartbeat
TIMER_SLEEP_THREAD = None # Thread for disconnection timer

IS_ELECTION_FINISHED = True # Determine if elestion is completed

# Determine server disconnection (simulation).
# If the server is unavailable, then it is not able to accept requests 
# and continue other processes on the server. This applies to both the 
# client and other servers.
IS_SERVER_UNAVAILABLE = False 

LOG = [] # server log -> [{index: 0, term: 0, command: ('set', 'key1', 'val1')}, ...]
COMMIT_INDEX = 0
LAST_APPLIED = 0
NEXT_INDEX = {}
MATCH_INDEX = {}


# Define server state
class ServerState(Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

# Method generates random timeout in range [150, 300]
def generateRandomTimeout():
    return random.randrange(150, 300) / 1000

# Method is used to stimulate disconnection in threads
def suspendServer():
    global IS_SERVER_UNAVAILABLE

    while IS_SERVER_UNAVAILABLE:
        continue

def replicateLog(new_log):
    global servers, TERM_NUMBER, SERVER_ID, COMMIT_INDEX, LOG, STATE, NEXT_INDEX, MATCH_INDEX

    print('Log replication', new_log)

    replication_success_num = 0
    for key in list(servers.keys()):
        try:
            server_channel = grpc.insecure_channel(servers[key])
            stub = pb2_grpc.ServerServiceStub(server_channel)

            message = pb2.AppendEntriesMessage()
            message.term = TERM_NUMBER
            message.leaderId = SERVER_ID
            message.prevLogIndex = MATCH_INDEX[key]
            message.prevLogTerm = 0 if len(LOG) == 0 else LOG[MATCH_INDEX[key]]['term']
            message.leaderCommit = COMMIT_INDEX
            
            command_message = pb2.CommandMessage(operation=new_log['command'][0], key=new_log['command'][1], value=new_log['command'][2])
            log_entry = pb2.LogEntry(index=new_log['index'], term=new_log['term'], command=command_message)

            message.entries.append(log_entry)

            response = stub.AppendEntries(message)

            print(response)

            if response.term > TERM_NUMBER:
                TIMER_THREAD.cancel()
                TERM_NUMBER = response.term
                STATE = ServerState.Follower

                print(f"I am follower. Term: {TERM_NUMBER}")

                TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
                TIMER_THREAD.start()
                break

            if response.success:
                replication_success_num += 1

                NEXT_INDEX[key] += 1
                MATCH_INDEX[key] = NEXT_INDEX[key] - 1

        except grpc.RpcError:
            continue
        except Exception as Error:
            print(Error)

    
    if replication_success_num > (len(list(servers.keys())) + 1) / 2:
        COMMIT_INDEX += 1


class ServerService(pb2_grpc.ServerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass

    def SetVal(self, request, context):
        global LOG, COMMIT_INDEX, LAST_APPLIED, STATE, TERM_NUMBER, LEADER
        try:
            if STATE.name == 'Candidate':
                return pb2.SetResponse(success=False)

            if STATE.name == 'Leader':
                newLongEntry = {'index': len(LOG), 'term': TERM_NUMBER, 'command': ('set', request.key, request.value)}

                replicateLog(newLongEntry)

                LOG.append()
                LAST_APPLIED += 1

                print(LOG)

                return pb2.SetResponse(success=True)
            else: # Follower case
                leader_channel = grpc.insecure_channel(servers[int(LEADER)])
                leader_stub = pb2_grpc.ServerServiceStub(leader_channel)

                message = pb2.SetKeyMessage(key=request.key, value=request.value)
                return leader_stub.SetVal(message)
        except Exception as Error:
            return pb2.SetResponse(success=False)
        
    def GetVal(self, request, context):
        global LOG

        print(LOG)

        target_value = None
        for entry in LOG:
            if (request.key == entry['command'][1]):
                target_value = entry['command'][2]

        return pb2.GetResponse(value=target_value)

    def GetLeader(self, request, context):
        global LEADER, servers, SERVER_ID, SERVER_ADDRESS, IS_SERVER_UNAVAILABLE

        if not IS_SERVER_UNAVAILABLE:
            print('Command from client: getleader')

            curr_Leader = LEADER
            address = None

            if LEADER is None: 
                curr_Leader = -1
                address = ''
            else:
                if LEADER == SERVER_ID:
                    address = SERVER_ADDRESS
                else:
                    address = servers[int(LEADER)]

            print(f'{curr_Leader} {address}')

            return pb2.LeaderResponse(id=curr_Leader, address=address)
    
    def AppendEntries(self, request, context):
        global TIMER_THREAD, TERM_NUMBER, STATE, TIMEOUT, LEADER, VOTED, IS_SERVER_UNAVAILABLE, LOG, COMMIT_INDEX

        if not IS_SERVER_UNAVAILABLE:
            # Reset timer only for Follower now
            if STATE.name == 'Follower':
                TIMER_THREAD.cancel()

            heartbeat_success = False
            if request.term >= TERM_NUMBER:
                heartbeat_success = True
                LEADER = request.leaderId
                
                # Write LOG
                if LOG[request.prevLogIndex] is None and len(LOG) > 0:
                    heartbeat_success = False
                else:
                    log_entry = request.entries[0]
                    log_command = log_entry.command

                    LOG.append({'index': log_entry.index, 'term': log_entry.term, 'command': (log_command.operation, log_command.key, log_command.value)})

                    COMMIT_INDEX = min(request.leaderCommit, LOG[-1]['index'])

                    print('Log update')
                    print(LOG)
                    print(f'Commit index: {COMMIT_INDEX}')

                if request.term > TERM_NUMBER: # Update own term
                    VOTED = None
                    TERM_NUMBER = request.term
                    print(f"I am a follower. Term: {TERM_NUMBER}")
                    
                    # Become follower due to higher Term
                    if STATE.name == 'Candidate' or STATE.name == 'Leader': # Become Follower if greater term is come
                        STATE = ServerState.Follower
                        print(f"I am a follower. Term: {TERM_NUMBER}")
                        TIMER_THREAD.cancel() # Cancel Timer for Candidate and Leader
            
            # Create new timer for Follower
            if STATE.name == 'Follower':
                TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
                TIMER_THREAD.start()

            return pb2.SuccessResponse(term=TERM_NUMBER, success=heartbeat_success)

    def RequestVote(self, request, context):
        global VOTED, TERM_NUMBER, STATE, TIMER_THREAD, TIMEOUT, LEADER, IS_SERVER_UNAVAILABLE

        if not IS_SERVER_UNAVAILABLE:
            voting_success = False
            my_term = TERM_NUMBER

            # Reset timer only for Follower now
            if STATE.name == 'Follower':
                TIMER_THREAD.cancel()

            if request.term >= TERM_NUMBER:
                if request.term > TERM_NUMBER:
                    TERM_NUMBER = request.term
                    VOTED = request.candidateId
                    voting_success = True

                    print(f'Voted for node {request.candidateId}')
                    
                    # Become follower due to higher Term
                    if STATE.name == 'Candidate' or STATE.name == 'Leader':
                        STATE = ServerState.Follower
                        print(f"I am a follower. Term: {TERM_NUMBER}")
                        TIMER_THREAD.cancel()
                        

                if VOTED is None:
                    VOTED = request.candidateId
                    voting_success = True

                    print(f'Voted for node {request.candidateId}')

            if STATE.name == 'Follower':
                TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
                TIMER_THREAD.start()

            return pb2.SuccessResponse(term=my_term, success=voting_success)
    
    def Suspend(self, request, context):
        global IS_SERVER_UNAVAILABLE, TIMER_SLEEP_THREAD

        if not IS_SERVER_UNAVAILABLE:
            print(f'Command from client: suspend {request.period}')

            IS_SERVER_UNAVAILABLE = True
            TIMER_SLEEP_THREAD = RaftTimer(request.period, server_wakeup)

            print(f'Sleeping for {request.period} seconds')
            TIMER_SLEEP_THREAD.start()

            return pb2.EmptyMessage()

class RaftTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

# Starts election after timeout
def start_election():
    global STATE, servers, TERM_NUMBER, SERVER_ID, TIMER_THREAD, VOTED, LEADER, TIMEOUT, IS_ELECTION_FINISHED, IS_SERVER_UNAVAILABLE

    print('The leader is dead')
    IS_ELECTION_FINISHED = False

    TIMER_THREAD.cancel() # Cancel previous timer
    TIMER_THREAD = RaftTimer(TIMEOUT, stop_election)

    STATE = ServerState.Candidate
    TERM_NUMBER = TERM_NUMBER + 1
    print(f"I am candidate. Term: {TERM_NUMBER}")

    votes = 1
    VOTED = SERVER_ID
    print(f'Voted for node {SERVER_ID}')

    # Start collecting votes
    for key in list(servers.keys()):
        try:
            if IS_ELECTION_FINISHED or IS_SERVER_UNAVAILABLE: # Election finished due to timer is up
                break

            server_channel = grpc.insecure_channel(servers[key])
            stub = pb2_grpc.ServerServiceStub(server_channel)

            message = pb2.RequestVoteMessage(term=TERM_NUMBER, candidateId=SERVER_ID)
            response = stub.RequestVote(message)

            if response.success:
                votes = votes + 1
            else:
                if response.term > TERM_NUMBER:
                    TERM_NUMBER = response.term

                    STATE = ServerState.Follower
                    print(f"I am a follower. Term: {TERM_NUMBER}")

                    break
        except grpc.RpcError:
            continue

    if STATE.name == 'Follower': # Candidate became Follower during election
        TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
        # print('Listen heartbeat - election')
    else:
        print('Votes recieved')

        win_number = (len(list(servers.keys())) + 1) / 2 

        if votes > win_number:    
            print(f"I am leader. Term: {TERM_NUMBER}")
            STATE = ServerState.Leader
            LEADER = SERVER_ID
            TIMER_THREAD = RaftTimer(0.05, send_heartbeat)
        else: # Candidate does not have the majority of votes
            STATE = ServerState.Follower
            TIMEOUT = generateRandomTimeout()
            TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
    
    TIMER_THREAD.start()
    VOTED = None

# Stop election when timer is up
def stop_election():
    global IS_ELECTION_FINISHED
    
    IS_ELECTION_FINISHED = True

# Sending heartbeat if Server is Leader
def send_heartbeat():
    global servers, TERM_NUMBER, SERVER_ID, TIMER_THREAD, STATE, TIMEOUT, IS_SERVER_UNAVAILABLE, COMMIT_INDEX, LOG

    if not IS_SERVER_UNAVAILABLE:
        for key in list(servers.keys()):
            try:
                server_channel = grpc.insecure_channel(servers[key])
                stub = pb2_grpc.ServerServiceStub(server_channel)

                message = pb2.AppendEntriesMessage()
                message.term = TERM_NUMBER
                message.leaderId = SERVER_ID
                message.prevLogIndex = 0 if len(LOG) == 0 else LOG[-1]['index']
                message.prevLogTerm = 0 if len(LOG) == 0 else LOG[-1]['term']
                message.leaderCommit = COMMIT_INDEX

                response = stub.AppendEntries(message)

                server_channel.close()

                if response.term > TERM_NUMBER:
                    TIMER_THREAD.cancel()
                    TERM_NUMBER = response.term
                    STATE = ServerState.Follower

                    print(f"I am follower. Term: {TERM_NUMBER}")

                    TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
                    TIMER_THREAD.start()
                    break

            except grpc.RpcError:
                continue

# Make Server is available after Suspend
def server_wakeup():
    global IS_SERVER_UNAVAILABLE, TIMER_SLEEP_THREAD

    IS_SERVER_UNAVAILABLE = False
    print('Server is Available')
    TIMER_SLEEP_THREAD.cancel()

def serve():
    global SERVER_Handler, STATE, TIMER_THREAD, SERVER_ID, TIMEOUT, SERVER_ADDRESS, TERM_NUMBER, servers, MATCH_INDEX, NEXT_INDEX
    SERVER_ID = int(sys.argv[1])

    STATE = ServerState.Follower
    TIMEOUT = generateRandomTimeout()

    # Read configuration file
    host = None
    port = None
    with open(config_file) as file:
        for line in file:
            tokens = line.split()

            if tokens[0] == str(SERVER_ID):
                if len(tokens) != 3:
                    error_msg = f'Wrong config format: {line}. It should has format:\nid host port'
                    raise Exception(error_msg)

                host = tokens[1]
                port = tokens[2]
            else:
                servers[int(tokens[0])] = f'{tokens[1]}:{tokens[2]}'
                MATCH_INDEX[int(tokens[0])] = 0
                NEXT_INDEX[int(tokens[0])] = 0
    
    # Configuration file doesn't includes required server id
    if host is None or port is None:
        print(f'{host}:{port}')
        raise Exception('Wrong port of host')
    
    SERVER_ADDRESS = f'{host}:{port}'

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServerServiceServicer_to_server(ServerService(), server)
    server.add_insecure_port(f'[::]:{port}')

    try:
        TIMER_THREAD = RaftTimer(TIMEOUT, start_election)
        print('Listen heartbeat - new')
        server.start()
        TIMER_THREAD.start()

        print(f'Server is started at {host}:{port}')
        print(f"I am a follower. Term: {TERM_NUMBER}")

        server.wait_for_termination()
    except KeyboardInterrupt:
        print('\nStopping server')
        TIMER_THREAD.cancel()

if __name__ == '__main__':
    serve()