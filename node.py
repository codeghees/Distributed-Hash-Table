import socket	
import numpy
import pickle
import random
import threading
import time
import queue 
import sys

def interface(port,myport):
    print("Interface thread is running")
    
    s = socket.socket()		 
    s.connect(("127.0.0.1", port))
    while True:
      connectionmsg = s.recv(1024)
      connectionmsg = pickle.loads(connectionmsg) 
      print(connectionmsg)
    
    print("Interface is over")
    s.close()
  

def main(port, otherport = None):
    if otherport != None:
        t1 = threading.Thread(target=interface, args=(otherport,port)) 
        t1.start()
        
    s = socket.socket()
    print ("Socket successfully created")
    s.bind(("", port))		 
    print ("socket binded to =" ,port)
    s.listen(10)	 
    print ("socket is listening")	
    while True:
        c, addr = s.accept()	 
        print(addr)
        print ('Got connection from', addr)
        c.send(pickle.dumps("Hello Paein"))
        
        
    t1.join()

        
     



if __name__=="__main__":
        myportnumber = int(sys.argv[1])
        knownport = int(input("Enter the port number if anyother port is known, -1 if None \npyth"))
        if knownport == -1:
                main(myportnumber)
        else:
                main(myportnumber,knownport)


