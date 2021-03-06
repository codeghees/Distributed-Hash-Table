import socket	
import numpy as np
import pickle
import random
import threading
import time
import queue 
import hashlib
import sys
import os
import time
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
      self.files = []
      
   def setsucc(self,succ):
           self.successor = succ
   def setpred(self,predd):
           self.pred = predd
   def CheckIfExist(self,Name):
           for item in self.files:
                if item == Name:
                        return True
           return False     

   def Leave(self):
           '''
           - Tell pred you are leaving
           - Tell Succ you are leaving
           - No need to worry about files
           '''
           if self.IfOnlyNode() == True:
                   print("Leaving")
                   exit()
           for files in self.files:
                   PutFilesSucc(files, self.successor, self.Port)
           #Deal with Successor
           SuccSock = socket.socket()
           SuccSock.connect((ip, self.successor))
           SuccSock.send(pickle.dumps("PredLeave"))
           Msg = pickle.loads(SuccSock.recv(1024))
           SuccSock.send(pickle.dumps(self.Port))
           Msg = pickle.loads(SuccSock.recv(1024))
           # Connection Made
           if Msg == "Leave":
                print("leaving")
                SuccSock.close()
           else:
                   SuccSock.send(pickle.dumps(self.pred))
                   SuccSock.close()
           #Deal with Pred
           PredSock = socket.socket()
           PredSock.connect((ip, self.pred))
           PredSock.send(pickle.dumps("SuccLeave"))
           Msg = pickle.loads(PredSock.recv(1024))
           PredSock.send(pickle.dumps(self.Port))
           # Connection Made
           PredSock.send(pickle.dumps(self.successor))
           PredSock.close()
           print("leaving")
           exit()

           


           return     
   def print(self):
           print("My pred is" , self.pred, " hash = ", hashPort(str(self.pred)))
           print("My succ is", self.successor, " hash = ", hashPort(str(self.successor)))
           print("My Second succ is", self.secSucc, " hash = ", hashPort(str(self.secSucc)))
           print("My port is ", self.Port, " hash = ", hashPort(str(self.Port)))
           print("My files are =", self.files)
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
   def IfFilePutHere(self,HashofFile):
           KnownPortKey = self.Hashkey
           PredPortKey = hashPort(str(self.pred))
           Ownkey = HashofFile
           
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
   def PutAtClient(self):
           '''
           -Running on Client
           -Get FileName from Client
           -Get Hash of file
           '''
           FileName = input("Enter Filename \n")
           HashFile = hashPort(FileName)
           print("HashofFile = ", HashFile)
           if os.path.exists(FileName):
                #    Fileobj = open(FileName,'r')
                   print("File has been read")
                   if self.IfFilePutHere(HashFile) == True:
                           if self.CheckIfExist(FileName) == False:
                                self.files.append(FileName)

                                print("Added to client")
                                PutFilesSucc(FileName, self.successor, self.Port) #SendFilesToSucc

                           else:
                                   print("File exists already")
                           return
                   else:
                           '''
                           -Open connection with Successor
                           -Tell to check if file can be stored
                           '''
                           print("Looking for succ")
                           NewThread = threading.Thread(target=PutSucc, args=(FileName,self.successor,self.Port)) 
                           NewThread.start()
           else:
                  print("File does not exist")
                


     
           return
   def KeepBackup(self,ClientNode):
           '''
           Runs on Server, the successor node.
           Keeps copies
           Put at server
           '''
           FileName = str(pickle.loads(ClientNode.recv(1024)))
           print("File - ",FileName)     
           HashFile = hashPort(FileName)
           print("Putting AT successor...")
           ClientNode.send(pickle.dumps("FileSend"))
           # READ FILE CODE GOES HERE
           # Tell Client to send file via sockets
           if self.CheckIfExist(FileName) == False:

        
                sizeofFile  = float(pickle.loads(ClientNode.recv(1024)))
                print("File size is = ", sizeofFile)
                W_byts  = ClientNode.recv(1024)
                Filen = os.path.join("backup",FileName)
                print("New Filename = ", Filen)
                FileOb = open(Filen,"wb")
                FileOb.write(W_byts)
                basesize = len(W_byts)
                while basesize<sizeofFile:
                        W_byts = ClientNode.recv(1024)
                        basesize = basesize + len(W_byts)
                        FileOb.write(W_byts)
                FileOb.close()
                self.files.append(FileName)        
                print("Added to Node")
           else:
                        print("File exists already")

        # self.files.append(FileName)
           print("File has been added")
           return
       
           



           
                   
   def GetAtClient(self):
           '''
           -Running on Client
           -Get FileName from Client
           -Get Hash of file
           - SEE IF AT CLIENT NODE
           - Check for succ
           '''
           FileName = input("Enter Filename to GET \n")
           HashFile = hashPort(FileName)
           print("HashofFile = ", HashFile)
           if FileName in self.files:
                #    Fileobj = open(FileName,'r')
                   print("File has been retrieved from Node")
                   
           else:
                  print("Looking for succ")
                  GetSucc(FileName,self.successor,self.Port) 
                #   print("File does not exist")
                


     
           return        
   def PutAtServer(self,ClientNode):
           '''
           -Running on Server
           -Get FileName from Client
           -Get Hash of file
           '''
           FileName = str(pickle.loads(ClientNode.recv(1024)))
           print("File - ",FileName)     
           HashFile = hashPort(FileName)
           if self.IfFilePutHere(HashFile) == True:
                print("Putting...")
                ClientNode.send(pickle.dumps("FileSend"))
                # READ FILE CODE GOES HERE
                # Tell Client to send file via sockets
                if self.IfFilePutHere(HashFile) == True:
                           if self.CheckIfExist(FileName) == False:

                                
                                sizeofFile  = float(pickle.loads(ClientNode.recv(1024)))
                                print("File size is = ", sizeofFile)
                                W_byts  = ClientNode.recv(1024)
                                Filen = os.path.join("Send",FileName)
                                print("New Filename = ", Filen)
                                FileOb = open(Filen,"wb")
                                FileOb.write(W_byts)
                                basesize = len(W_byts)
                                while basesize<sizeofFile:
                                        W_byts = ClientNode.recv(1024)
                                        basesize = basesize + len(W_byts)
                                        FileOb.write(W_byts)
                                FileOb.close()
                                self.files.append(FileName)        
                                print("Added to Node")
                           else:
                                   print("File exists already")
                     
                # self.files.append(FileName)
                print("File has been added")
                PutFilesSucc(FileName, self.successor, self.Port) #SendFilesToSucc

                return
           else:
                '''
                -Open connection with Successor
                -Tell to check if file can be stored
                '''
                print("Put Request forwarded to Successor")
                ClientNode.send(pickle.dumps("Sending Successor"))
                Msg = str(pickle.loads(ClientNode.recv(1024)))
                if Msg == "OK_S":
                        ClientNode.send(pickle.dumps(self.successor))
                

           return
        

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
   def GetFromServer(self,ClientNode):
           '''
           Running on the server WHICH HAS THE FILE
           '''
           FileName = str(pickle.loads(ClientNode.recv(1024)))
           print("File - ",FileName)     
           HashFile = hashPort(FileName)
           if self.IfFilePutHere(HashFile) == True:
                print("Getting...")
                
                if self.IfFilePutHere(HashFile) == True:
                           if self.CheckIfExist(FileName) == True:
                                ClientNode.send(pickle.dumps("Sending"))
                  
                                size = os.path.getsize(FileName)
                                ClientNode.send(pickle.dumps(size)) #Send File
                                print("Size of File sent", size)
                                
                                if os.path.exists(FileName):
                                        FileObj = open(FileName,"rb")
                                
                                        byts = FileObj.read(1024)
                                        ClientNode.send(byts)
                                        while (byts):
                                                byts = FileObj.read(1024)
                                                ClientNode.send(byts)
                                        FileObj.close()
                                        print("Sent to Node")
                                        print("File has been sent")

                                else:
                                        print("File Does Not Exist")
                           else:
                                   print("File does not exists")
                                   ClientNode.send(pickle.dumps("FileNot"))
                     
                # self.files.append(FileName)
                return
           else:
                '''
                -Open connection with Successor
                -Tell to check if file can be stored
                '''
                print("Put Request forwarded to Successor")
                ClientNode.send(pickle.dumps("Sending Successor"))
                Msg = str(pickle.loads(ClientNode.recv(1024)))
                if Msg == "OK_S":
                        ClientNode.send(pickle.dumps(self.successor))
                

           return
        


           return
def GetSucc(FileName,successor,Port):
        '''
        - Running on Client
        - Looks for successor
        - Gets file from it
        '''
        while True:
                SockSuc = socket.socket()
                SockSuc.connect((ip,successor))
                SockSuc.send(pickle.dumps("GetReq"))
                Msg = pickle.loads(SockSuc.recv(1024))
                if Msg == "Connected":
                        SockSuc.send(pickle.dumps(Port))
                        SockSuc.send(pickle.dumps(FileName))
                        Msg2 = pickle.loads(SockSuc.recv(1024))
                        if Msg2 == "Sending":
                                print("MSG2 received")
                                #Recv File code goes here
                                
                                sizeofFile  = float(pickle.loads(SockSuc.recv(1024)))
                                print("File size is = ", sizeofFile)
                                W_byts  = SockSuc.recv(1024)
                                Filen = os.path.join("Recv",FileName)
                                print("New Filename = ", Filen)
                                FileOb = open(Filen,"wb")
                                FileOb.write(W_byts)
                                basesize = len(W_byts)
                                while basesize<sizeofFile:
                                        W_byts = SockSuc.recv(1024)
                                        basesize = basesize + len(W_byts)
                                        FileOb.write(W_byts)
                                FileOb.close()
                                # self.files.append(FileName)        
                                

                                ##
                                        
                                return
                        elif Msg2 == "Sending Successor":
                                print("Req forwarded, waiting for Succ")
                                SockSuc.send(pickle.dumps("OK_S"))
                                successor = int(pickle.loads(SockSuc.recv(1024)))
                                print("Succ received")
                        elif Msg2 == "FileNot":
                                print("No file")
                                return









def PutSucc(Filename, SuccPort, ServerPort):
        '''
        - Running On Client
        - Send File
        '''
        while True:
                SockSuc = socket.socket()
                SockSuc.connect((ip,SuccPort))
                SockSuc.send(pickle.dumps("PutReq"))
                Msg = pickle.loads(SockSuc.recv(1024))
                if Msg == "Connected":
                        SockSuc.send(pickle.dumps(ServerPort))
                        SockSuc.send(pickle.dumps(Filename))
                        Msg2 = pickle.loads(SockSuc.recv(1024))
                        if Msg2 == "FileSend":
                                print("MSG2 received")
                                #Sending File code goes here
                                size = os.path.getsize(Filename)
                                SockSuc.send(pickle.dumps(size)) #Send File
                                print("Size of File sent", size)
                                
                                FileObj = open(Filename,"rb")
                                byts = FileObj.read(1024)
                                SockSuc.send(byts)
                                while (byts):
                                        byts = FileObj.read(1024)
                                        SockSuc.send(byts)
                                FileObj.close()        
                                return
                        elif Msg2 == "Sending Successor":
                                print("Req forwarded, waiting for Succ")
                                SockSuc.send(pickle.dumps("OK_S"))
                                SuccPort = int(pickle.loads(SockSuc.recv(1024)))
                                print("Succ received")






def PutFilesSucc(Filename, SuccPort, ServerPort):
        '''
        - Running On Client
        - Send File
        - Sends files to successor for back up
        '''
        while True:
                SockSuc = socket.socket()
                SockSuc.connect((ip,SuccPort))
                SockSuc.send(pickle.dumps("PutSucc"))
                Msg = pickle.loads(SockSuc.recv(1024))
                if Msg == "Connected":
                        SockSuc.send(pickle.dumps(ServerPort))
                        SockSuc.send(pickle.dumps(Filename))
                        Msg2 = pickle.loads(SockSuc.recv(1024))
                        if Msg2 == "FileSend":
                                print("MSG2 received")
                                #Sending File code goes here
                                size = os.path.getsize(Filename)
                                SockSuc.send(pickle.dumps(size)) #Send File
                                print("Size of File sent", size)
                                
                                FileObj = open(Filename,"rb")
                                byts = FileObj.read(1024)
                                SockSuc.send(byts)
                                while (byts):
                                        byts = FileObj.read(1024)
                                        SockSuc.send(byts)
                                FileObj.close()        
                                return
                        elif Msg2 == "Sending Successor":
                                print("Req forwarded, waiting for Succ")
                                SockSuc.send(pickle.dumps("OK_S"))
                                SuccPort = int(pickle.loads(SockSuc.recv(1024)))
                                print("Succ received")








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
def UpdateSecondSuccs(ClientNode,ServerNodeClass):
        '''
        First you need to update second Successor of your own
        then update Second Succ of your pred
        '''
        SuccSocket =  socket.socket()
        SuccSocket.connect((ip,ServerNodeClass.successor))
        SuccSocket.send(pickle.dumps("SS"))
        Msg = SuccSocket.recv(1024)
        print(pickle.loads(Msg))
        SuccSocket.send(pickle.dumps(ServerNodeClass.Port))
        SecondSuccessor = int(pickle.loads(SuccSocket.recv(1024)))
        ServerNodeClass.secSucc = SecondSuccessor

        SuccSocket.close()
        print("Updating Second Succ of (pred)", ServerNodeClass.pred)
        PredSocket = socket.socket()
        PredSocket.connect((ip,ServerNodeClass.pred))
        PredSocket.send(pickle.dumps("SSS"))
        Msg = PredSocket.recv(1024)
        print(pickle.loads(Msg))
        PredSocket.send(pickle.dumps(ServerNodeClass.Port))
        PredSocket.send(pickle.dumps(ServerNodeClass.successor))
        PredSocket.close()

        return
def UpdateServerLinks(ClientNode,ServerNodeClass):
        print("My Successor is updating...")
        # ServerNodeClass.print()
        NewSuccessor = int(pickle.loads(ClientNode.recv(1024)))
        print("XXX")


        ServerNodeClass.setsucc(NewSuccessor)
        ClientNode.send(pickle.dumps("SuccessorUpdated"))
        UpdateSecondSuccs(ClientNode,ServerNodeClass) # This updates second successor for both the current node AND the predecessor of current node

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
def SendFiles(ServerNodeClass):
        '''
        -Send Files to Pred
        '''
        for filee in ServerNodeClass.files:
                FileHash = hashPort(filee)
                predhash = hashPort(str(ServerNodeClass.pred))
                if FileHash<predhash:
                        PutSucc(filee,ServerNodeClass.pred, ServerNodeClass.Port)
                        # ServerNodeClass.files.remove(filee)

def updateSecondSuccessor(ClientNode,ServerNodeClass):
        print("My Second Successor is being updated")
        SecondSucc = int(pickle.loads(ClientNode.recv(1024)))
        ServerNodeClass.secSucc = SecondSucc
        return
def sendSuccessor(ClientNode, ServerNodeClass):
        ClientNode.send(pickle.dumps(ServerNodeClass.successor))
        return
def ServerThread(ClientNode, ServerNodeClass):
        #Running on Server, updates the Server Class Node
        Message = pickle.loads(ClientNode.recv(1024))
        ClientNode.send(pickle.dumps("Connected"))
        Port = int(pickle.loads(ClientNode.recv(1024)))
        print("Connection from = ", Port)
           
        if Message == "JoinRequest":
                JoinDHT(ClientNode,Port,ServerNodeClass)
                UpdateSecondSuccs(ClientNode,ServerNodeClass) # This updates second successor for both the current node AND the predecessor of current node
                SendFiles(ServerNodeClass)
        elif Message == "UpdateServer":
                print("Called by ", Port)
                UpdateServerLinks(ClientNode,ServerNodeClass)

        elif Message == "SS":
                sendSuccessor(ClientNode,ServerNodeClass)
        elif Message == "SSS":
                updateSecondSuccessor(ClientNode,ServerNodeClass)
        elif Message == "PutReq":
                print("Put Req made by = ", Port)
                ServerNodeClass.PutAtServer(ClientNode)
        elif Message == "GetReq":
                print("Get Req made by = ", Port)
                ServerNodeClass.GetFromServer(ClientNode)
        elif Message == "PutSucc":
                print("Put Req Succ made by = ", Port)
                ServerNodeClass.KeepBackup(ClientNode)
        elif Message == "PredLeave":
                #Pred leaving
                #Check if Only one node left
                if (ServerNodeClass.successor == ServerNodeClass.pred) and (ServerNodeClass.successor == Port):
                        ServerNodeClass.successor = ServerNodeClass.Port
                        ServerNodeClass.pred = ServerNodeClass.Port
                        print("ServerReset")
                        ClientNode.send(pickle.dumps("Leave"))
                else:
                        print("SEND PRED b4 leaving")
                        ClientNode.send(pickle.dumps("SendPred"))
                        predd = pickle.loads(ClientNode.recv(1024))
                        ServerNodeClass.pred = predd
                        UpdateSecondSuccs(ClientNode,ServerNodeClass)
        elif Message == "SuccLeave":
                succ = pickle.loads(ClientNode.recv(1024))
                ServerNodeClass.successor = succ
                UpdateSecondSuccs(ClientNode,ServerNodeClass)
        elif Message == "Ping":
                ClientNode.send(pickle.dumps("Ping"))
        elif Message == "NewPred":
                ServerNodeClass.pred = Port


        print("Server info:")
        ServerNodeClass.print()
        print("Connected with = ", Port )
        ClientNode.close()
        return
def Ping(NodeObj):
        count = 0
        while True:
               time.sleep(5)

               if NodeObj.IfOnlyNode() == False: 
                        try:
                                sock =  socket.socket()
                                sock.connect((ip,NodeObj.successor))
                                sock.send(pickle.dumps("Ping"))
                                Ack = pickle.loads(sock.recv(1024))
                                sock.send(pickle.dumps(NodeObj.Port))
                                Msg = pickle.loads(sock.recv(1024))
                                if Msg == "Ping":
                                        count = 0
                                sock.close()
                        except socket.error as err:
                                print("ping failed", err)
                                count = count + 1
                        if count == 3:
                                sock = socket.socket()
                                sock.connect((ip,NodeObj.secSucc))
                                sock.send(pickle.dumps("NewPred"))
                                Msg = pickle.loads(sock.recv(1024))
                                sock.send(pickle.dumps(NodeObj.Port))
                                NodeObj.successor = NodeObj.secSucc
                                UpdateSecondSuccs(sock,NodeObj)
                                for files in NodeObj.files:
                                        PutFilesSucc(files, NodeObj.successor, NodeObj.Port)

                                


        return
def ClientInterface(NodeObj):

        # F _ R
        Client = threading.Thread(target=Ping, args=(NodeObj,)) 
        Client.start()
          
        while True:
                Query = (input("Enter your query \n 1.Put\n 2.Get\n 3.Print 4. Leave\n"))
                if Query == "1":
                        NodeObj.PutAtClient()
                elif Query == "2":
                        NodeObj.GetAtClient()
                elif Query == "3":
                        NodeObj.print()
                elif Query == "4":
                        NodeObj.Leave()
                        return
                else:
                        continue
                        
                        
                

                   


def main(port, otherport = None):

    NodeObj = Node(port)
    print(NodeObj.successor)
    if otherport != None:
        NodeObj.AddtoDHT(otherport)
    #Start Listening   
    Client = threading.Thread(target=ClientInterface, args=(NodeObj,)) 
    Client.start()
                  
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
        
        knownport = int(input("Enter the port number if anyother port is known, -1 if None \n"))
        if knownport == -1:
                main(myportnumber)
        else:
                main(myportnumber,knownport)



'''
Things to do:
- Hashing
- Testing for two Nodes DONE
- Join Three nodes DONE
- Complete ring DONE
- Second Successor DONE
- Files get and Put DONE
- Send files to pred on connection DONE
- Leaving 
        - Make a function that sends files to successor
        - call it during join
        
- Failure DONE 
'''