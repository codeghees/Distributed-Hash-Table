import socket	
import numpy as np
import pickle
import random
import threading
import time
import queue 
import hashlib
import sys
M = 5
ip = "127.0.0.1"
class Node:
   def __init__(self, Port):
      self.Port = Port
      self.successor = Port
      self.pred = Port
      self.secSucc = Port
      self.Hashkey = hashPort(str(Port))
      self.fingerTable = np.ones((M,2))
   def setsucc(self,succ):
           self.successor = succ
   def setpred(self,predd):
           self.pred = predd

   def print(self):
           print("My pred is" , self.pred)
           print("My succ is", self.successor)
           print("My port is ", self.Port)
           print("\n")
   def IfSpaceFound(self,KnownPort,PredPort):
           KnownPortKey = hashPort(str(KnownPort))
           PredPortKey = hashPort(str(PredPort))
           Ownkey = self.Hashkey
           
           if Ownkey < KnownPortKey:
                   if (KnownPortKey>PredPortKey) and (Ownkey > PredPortKey):
                        #Handle The case when no looping around linear chain basically
                           return True
                   if (KnownPortKey < PredPortKey) and (PredPortKey > Ownkey):
                        #Handle The case when looping around
                           return True

           elif Ownkey > KnownPortKey:
                   if (KnownPortKey<PredPortKey) and (Ownkey > PredPortKey):
                        #Handle The other half of the loop
                           return True


           return False                        
   def AddtoDHT(self,NewPort):
           #Client is Requesting to connect with DHT
           #Client is running
           Client = socket.socket()
           Client.connect((ip, NewPort))
           Client.send(pickle.dumps("JoinRequest"))
           print("We got here")
           ConnectMsg = pickle.loads(Client.recv(1024))
           print(ConnectMsg)
           if ConnectMsg == "Connected":
              Client.send(pickle.dumps(self.Port))
              OnlyNode = str(pickle.loads(Client.recv(1024)))
              #If the first Node
              if OnlyNode == "FirstNode":
                      self.successor = NewPort
                      self.pred = NewPort
                      print("Two Nodes Connected")
                      self.print()
              elif OnlyNode == "Ring":
                      # Not the first node
                      # Write join conditions
                      Client.send(pickle.dumps("Send Pred"))
                      PredPort = int(pickle.loads(Client.recv(1024)))
                      '''
                      First check if it lies between pred and known node
                      Else RECURSIVELY Call on the next available node 
                      '''
                      if self.IfSpaceFound(NewPort,PredPort) == True:
                              self.successor =  NewPort
                              self.pred = PredPort
                              Client.send(pickle.dumps("Connected"))

                      else:
                              print("Finding Successor of Known Node")
                              Client.send(pickle.dumps("SendSucc"))
                              SuccPort = int(pickle.loads(Client.recv(1024)))
                              self.AddtoDHT(SuccPort)
                      
           else:
                Client.close()   
                return   
           self.print()
           Client.close()
           return
        

   def IfOnlyNode(self):
           #First Node Check
           if self.successor == self.Port and self.pred == self.Port :
                   print("I am the only Node")
                   return True
           return False
def UpdateServer(ServerNodeClass,Port):
        '''
        Open Connection with Predecessor of Current Server, tell it about it's new Successor
        Also update the predeccesor of the Server
        '''
        PredSocket = socket.socket()
        PredSocket.connect((ip,ServerNodeClass.pred))
        print("Connecting with Pred", ServerNodeClass.pred)
        PredSocket.send(pickle.dumps("UpdateServer"))
        ACK = pickle.loads(PredSocket.recv(1024))
        print("ACK = ", ACK)
        PredSocket.send(pickle.dumps(ServerNodeClass.Port))
        print("X")
        PredSocket.send(pickle.dumps(Port))
        Msg = pickle.loads(PredSocket.recv(1024))
        if Msg == "SuccessorUpdated":
                print("Succ of Pred updated \n updating my pred")
                ServerNodeClass.pred = Port
        PredSocket.close()
        return
def UpdateServerLinks(ClientNode,ServerNodeClass):
        print("My Successor is updating...")
        # ServerNodeClass.print()
        NewSuccessor = int(pickle.loads(ClientNode.recv(1024)))
        print("XXX")


        ServerNodeClass.setsucc(NewSuccessor)
        ClientNode.send(pickle.dumps("SuccessorUpdated"))
        ClientNode.close()
        return
def JoinDHT(ClientNode,Port,ServerNodeClass):
        #Makes the ring
        #Running on the server
        if ServerNodeClass.IfOnlyNode() == True:
                ClientNode.send(pickle.dumps("FirstNode"))
                ServerNodeClass.setsucc(Port)
                ServerNodeClass.setpred(Port)
                ServerNodeClass.print()
        else:
                ClientNode.send(pickle.dumps("Ring"))
                Msg = pickle.loads(ClientNode.recv(1024))
                print(Msg)
                # Sending Pred
                if Msg == "Send Pred":
                        Pred = ServerNodeClass.pred
                        ClientNode.send(pickle.dumps(Pred))
                        Msg2 = pickle.loads(ClientNode.recv(1024))
                        if Msg2 == "Connected":
                                print("Connection made")
                                UpdateServer(ServerNodeClass,Port)
                        if Msg2 == "SendSucc":
                                ClientNode.send(pickle.dumps(ServerNodeClass.successor))

                                
                        

        ServerNodeClass.print()
        return
def ServerThread(ClientNode, ServerNodeClass):
        #Running on Server, updates the Server Class Node
        Message = pickle.loads(ClientNode.recv(1024))
        ClientNode.send(pickle.dumps("Connected"))
        Port = int(pickle.loads(ClientNode.recv(1024)))
        print("Connection from = ", Port)
           
        if Message == "JoinRequest":
                JoinDHT(ClientNode,Port,ServerNodeClass)
        elif Message == "UpdateServer":
                print("Called by ", Port)
                UpdateServerLinks(ClientNode,ServerNodeClass)
        ServerNodeClass.print()
        print("Connected with = ", Port )
        ClientNode.close()
        return
  


def main(port, otherport = None):
    global NodeObj

    NodeObj = Node(port)
    print(NodeObj.successor)
    if otherport != None:
        NodeObj.AddtoDHT(otherport)
    #Start Listening     
    s = socket.socket()
    print ("Socket successfully created")
    s.bind(("", port))		 
    print ("socket binded to =" ,port)
    s.listen(100)	 
    print ("socket is listening")
    while True:
                c, addr = s.accept()	 
                print(addr)
                print ('Got connection from', addr)
                Server = threading.Thread(target=ServerThread, args=(c,NodeObj)) 
                Server.start()
                
    Server.join()   
          
    
def hashPort(hashStr):
        hash_object = hashlib.sha1(hashStr.encode())
        hex_dig = hash_object.hexdigest()
        intval = int(hex_dig, 16) % 40
        print(intval)
        return intval



     

if __name__ == '__main__':
        
        myportnumber = int(sys.argv[1])
        hashPort(str(myportnumber))
        
        knownport = int(input("Enter the port number if anyother port is known, -1 if None \npyth"))
        if knownport == -1:
                main(myportnumber)
        else:
                main(myportnumber,knownport)



'''
Things to do:
- Hashing
- Testing for two Nodes DONE
- Join Three nodes
- Complete ring
'''