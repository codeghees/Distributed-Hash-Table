import socket	
import numpy
import pickle
import random
import threading
import time
import queue 
import sys
M = 5
ip = "127.0.0.1"
class Node:
   def __init__(self, Port):
      self.Port = Port
      self.successor = Port
      self.pred = Port
      self.secSucc = Port
      self.fingerTable = {}
   def setsucc(self,succ):
           self.successor = succ
   def AddtoDHT(self,NewPort):
           #Client is Requesting to connect with DHT
           Client = socket.socket()
           Client.connect(("", NewPort))
           Client.send("JoinRequest")
           ConnectMsg = Client.recv(1024)
           print(ConnectMsg)
           if ConnectMsg == "Connected":
              Client.send(str(NewPort))
              OnlyNode = str(Client.recv(1024))
              if OnlyNode == "FirstNode":
                      self.successor = NewPort
                      self.pred = NewPort

           else:
                return
                #Not The Only Node
        



   def IfOnlyNode(self):
           if self.successor == self.Port and self.pred == self.Port :
                   return True
           return False
def ServerThread(ClientNode, ServerNodeClass):
     #Add check if Only node in client
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

          
    
        
     

if __name__ == '__main__':
        myportnumber = int(sys.argv[1])
        knownport = int(input("Enter the port number if anyother port is known, -1 if None \npyth"))
        if knownport == -1:
                main(myportnumber)
        else:
                main(myportnumber,knownport)


