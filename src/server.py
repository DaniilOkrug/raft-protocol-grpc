import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
import grpc
import sys

from concurrent import futures

config_file = 'config.conf'

class ServerService(pb2_grpc.ServerServiceServicer):
    def __init__(self, *args, **kwargs):
        pass
    
def serve():
    server_id = sys.argv[1]

    # Read configuration file
    host = None
    port = None
    with open(config_file) as file:
        for line in file:
            tokens = line.split()

            if tokens[0] == server_id:
                if len(tokens) != 3:
                    error_msg = f'Wrong config format: {line}. It should has format:\nid host port'
                    raise Exception(error_msg)

                host = tokens[1]
                port = tokens[2]
                break
    
    # Configuration file doesn't includes required server id
    if host is None or port is None:
        print(f'{host}:{port}')
        raise Exception('Wrong port of host')

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ServerServiceServicer_to_server(ServerService(), server)
    server.add_insecure_port(f'[::]:{port}')

    server.start()

    print(f'Server is started at {host}:{port}')

    server.wait_for_termination()

if __name__ == '__main__':
    serve()