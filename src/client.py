import raft_pb2_grpc as pb2_grpc
import raft_pb2 as pb2
import grpc

class Client(object):
    def __init__(self):
        self.host = 'localhost'
        self.channel = None
        self.stub = None
        self.address = None
    
    def connect(self, address):
        try:
            self.address = address
            self.channel = grpc.insecure_channel(self.address)
            self.stub = pb2_grpc.ServerServiceStub(self.channel)
            
            message = pb2.EmptyMessage()
            response = self.stub.GetLeader(message) # Request getting leader for checking connection
        except grpc.RpcError:
            print(f'The server {self.address} is unavailable')
        except Exception as Error:
            print(Error)
            
    def getleader(self):
        try:
            message = pb2.EmptyMessage()
            response = self.stub.GetLeader(message)

            print(f'{response.id} {response.address}')
        except grpc.RpcError:
            print(f'The server {self.address} is unavailable')
        except Exception as Error:
            print(Error)

    def suspend(self, period):
        try:
            message = pb2.SuspendMessage(period=int(period))
            response = self.stub.Suspend(message)
        except grpc.RpcError:
            print(f'The server {self.address} is unavailable')
        except Exception as Error:
            print(Error)

    def setval(self, key, value):
        try:
            message = pb2.SetKeyMessage(key=key, value=value)
            response = self.stub.SetVal(message)
        except grpc.RpcError:
            print(f'The server {self.address} is unavailable')
        except Exception as Error:
            print(Error)
    
    def getval(self, key):
        try:
            message = pb2.GetValMessage(key=key)
            response = self.stub.GetVal(message)
            
            if response.value == '':
                print('None')
            else:
                print(response.value)
        except grpc.RpcError:
            print(f'The server {self.address} is unavailable')
        except Exception as Error:
            print(Error)
    
    def quit(self):
        if self.channel is not None:
            self.channel.close()
            self.channel = None

if __name__ == '__main__':
    client = Client()
    print('The client starts')

    while True:
        try:
            msg = input('> ')

            words = msg.split()
            if len(words) == 0:
                print('')
                continue

            if words[0] == 'connect' and len(words) == 2: # connect command
                client.connect(words[1])
            elif words[0] == 'getleader' and len(words) == 1: # getleader command
                client.getleader()
            elif words[0] == 'suspend' and len(words) == 2: # suspend command
                client.suspend(words[1])
            elif words[0] == 'setval' and len(words) == 3: # setval command
                client.setval(words[1], words[2])
            elif words[0] == 'getval' and len(words) == 2: # getval command 
                client.getval(words[1])
            elif words[0] == 'quit' and len(words) == 1: # quit command
                client.quit()
                break

        except KeyboardInterrupt:
            break

    print('\nThe client ends working')