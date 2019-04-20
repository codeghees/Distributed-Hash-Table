import socket	
import numpy
import pickle
import random
import threading
import time
import queue 
import sys

fingertable = {}
succ = 2
m = 5

pred = 1
def listening():
    s = socket.socket()
    print ("Socket successfully created")
    port = int(sys.argv[1])
    s.bind(("127.0.0.1", port))		 
    print ("socket binded to =" ,port)
    s.listen(5)	 
    print ("socket is listening")	
    while True:
        c, addr = s.accept()	 
        print(c)
        print ('Got connection from', addr)
     

def interface(port,myport):
    print("Interface thread is running")
    
    s = socket.socket()		 
    s.connect((sys.argv[1], port)) 
    while True:
        x = 0
        x+=1
    print("Interface is over")

def main(port, otherport = None):
    if otherport != None:
        t1 = threading.Thread(target=interface, args=(otherport,port)) 
        t1.start()
        t1.join()
    s = socket.socket()
    print ("Socket successfully created")
    s.bind(("", port))		 
    print ("socket binded to =" ,port)
    s.listen(10)	 
    print ("socket is listening")	
    while True:
        c, addr = s.accept()	 
        print(c)
        print ('Got connection from', addr)
     



if __name__=="__main__":
        myportnumber = int(sys.argv[1])
        knownport = int(input("Enter the port number if anyother port is known, -1 if None"))
        if knownport == -1:
                main(myportnumber)
        else:
                main(myportnumber,knownport)


