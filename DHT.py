import socket
import thread
import sys
import hashlib
import math
import os
filenames = []
ID = 0
suc = (0,"")
suc1 = (0,"")
pred = (0,"")
pred1 = (0,"")
address = ""
port = 0
constant = int(math.pow(2,8))

def lookup(hashID, check):
	budAdd = ""
	if suc[0]==pred[0] and ID> hashID and hashID > suc[0]:
		return str(ID) + '/' + address
	elif suc[0]==pred[0] and ID< hashID and hashID < suc[0]:
		return str(suc[0]) + '/' + suc[1]	
	elif suc[0]==pred[0] and hashID < ID and hashID < suc[0] and ID > suc[0]:
		return str(suc[0]) + '/' + suc[1]	
	elif suc[0]==pred[0] and hashID < ID and hashID < suc[0] and ID  < suc[0]:
		return str(ID) + '/' + address
	elif hashID > ID and hashID < suc[0]:
		return str(suc[0]) + '/' + suc[1]	
	elif ID > hashID and pred[0]< hashID:
		return str(ID) + '/' + address
	elif ID > hashID and pred[0] > ID:
		return str(ID) + '/' + address
	elif ID > hashID:
		budAdd = pred[1]
	elif suc[0] > hashID:
		return str(suc[0]) + '/' + suc[1]
	elif suc[0] < ID and hashID > suc[0]:
		return str(suc[0]) + '/' + suc[1]
	elif hashID> suc[0]:
		budAdd = suc[1]
	else:
		budAdd = suc[1]
	ip=budAdd.split(':')[0] 
	port1= budAdd.split(':')[1]
	if port1==port:
		return str(ID) + '/' + address
	s = socket.socket() 
	s.connect((ip,int(port1)))
	s.send('LOOK/' + str(hashID) +'/')
	lol = s.recv(1024)
	s.close()
	return lol






def interface(s):
	while True:
		menu = """What operation would you like to perform
		1) Join another peer
		2) Leave the network
		3) Download a file
		4) Upload a file"""

		option = raw_input(menu +'\n')
		if(not (option in ["1","2","3","4"])):
			print "invalid option"

		else:
			if(option == "1"):
				join()
			if option =="2":
				leave()
				break
			elif option == "3":
				download()
			elif(option == "4"):
				upload()
		print ID
		print suc
		print pred
def leave():
	sucPort = suc[1].split(':')[1]
	predPort = pred[1].split(':')[1]
	first = socket.socket()
	first.connect(('127.0.0.1',int(sucPort)))
	first.send("SUCC" + '/' + predPort) 
	first.recv(1024)
	first.close()
	second = socket.socket()
	second.connect(('127.0.0.1',int(predPort)))
	second.send("PRED"+ '/' + sucPort)
	second.recv(1024)
	second.close()
	usocket = socket.socket()
	usocket.connect(('127.0.0.1', int(sucPort)))	
	for filename in filenames:
		try : 
			f = open(filename,'rb')
		except:
			break
		usocket.send("UPLOAD")
		usocket.recv(1024)
		usocket.send(filename)
		filesize = str(os.path.getsize(filename))
		usocket.recv(1024)
		usocket.send(filesize)
		filetosend = f.read(1024)
		usocket.send(filetosend)
		while filetosend != "":
			filetosend = f.read(1024)
			usocket.send(filetosend)
		usocket.recv(1024)


def download():
	filename = raw_input("Enter name of the file you want to download: ")
	hashID = hashlib.sha1(filename).hexdigest()
	hashID = int(hashID,16)
	hashID = hashID%constant
	print hashID
	search = lookup(hashID,True)
	print search
	uPort = search.split('/')[1].split(':')[1]
	if uPort == port:
		print "Already here"
		return
	usocket = socket.socket()
	usocket.connect(('127.0.0.1', int(uPort)))
	usocket.send("DOWNLOAD")
	usocket.recv(1024)
	usocket.send(filename)
	filesize = usocket.recv(1024)
	if "NOT" in filesize:
		print "File not in system"
		return
	filesize= int(filesize)
	usocket.send("lol")
	data = usocket.recv(1024)
	totalRecv = len(data)
	f = open(filename, 'wb')
	f.write(data)
	while totalRecv < filesize:
		data = usocket.recv(1024)
		totalRecv += len(data)
		f.write(data)

def downloader(client):
	client.send("lol")
	filename = client.recv(1024)
	try:
		f = open(filename,'rb')
	except:
		client.send("NOT")
		return
	filesize = str(os.path.getsize(filename))
	client.send(filesize)
	client.recv(1024)
	filetosend = f.read(1024)
	client.send(filetosend)
	while filetosend != "":
		filetosend = f.read(1024)
		client.send(filetosend)


def upload():
	filename = raw_input("Enter name of the file you want to upload: ")
	try:
		f = open(filename,'rb')
	except:
		upload()
		return
	hashID = hashlib.sha1(filename).hexdigest()
	hashID = int(hashID,16)
	hashID = hashID%constant
	print hashID
	search = lookup(hashID,True)
	print search
	uPort = search.split('/')[1].split(':')[1]
	if uPort == port:
		print "Already here"
		filenames.append(filename)
		return
	usocket = socket.socket()
	usocket.connect(('127.0.0.1', int(uPort)))
	usocket.send("UPLOAD")
	usocket.recv(1024)
	usocket.send(filename)
	filesize = str(os.path.getsize(filename))
	usocket.recv(1024)
	usocket.send(filesize)
	filetosend = f.read(1024)
	usocket.send(filetosend)
	while filetosend != "":
		filetosend = f.read(1024)
		usocket.send(filetosend)
	usocket.recv(1024)

def uploader(client):
	client.send("OK")
	filename = client.recv(1024)
	client.send("ok")
	filesize = int(client.recv(1024))

	f = open(filename, 'wb')
	client.send("SEND/")
	filenames.append(filename)
	data = client.recv(1024)
	totalRecv = len(data)
	f.write(data)
	while totalRecv < filesize:
		data = client.recv(1024)
		totalRecv += len(data)
		f.write(data)
	client.send("ok")

def join():
	global suc
	global pred
	global suc1
	global pred1
	nodexip = raw_input("please enter IP address of node to connect to: ")
	if(nodexip != '127.0.0.1'):
		print "false IP"
		return
	nodexport = raw_input("please enter port of node to connect to: ")
	peerid = hashlib.sha1('127.0.0.1'+':'+ nodexport).hexdigest()
	peerid = int(peerid, 16)
	peerid = peerid%constant
	s = socket.socket() 
	# try :
	print peerid
	s.connect(('127.0.0.1',int(nodexport)))
	s.send("CONN" + '/' + str(port))
	sucessor = s.recv(1024)
	suc = (int(sucessor.split('/')[0]),sucessor.split('/')[1])
	print suc
	s.close()
	sucPort = suc[1].split(':')[1]
	first = socket.socket()
	first.connect(('127.0.0.1',int(sucPort)))
	first.send("SUCC" + '/' + str(port))
	predecessor = first.recv(1024)
	first.close()
	################first sucessor done ############ 
	pred = (int(predecessor.split('/')[0]),predecessor.split('/')[1])
	predPort = pred[1].split(':')[1]
	second = socket.socket()
	second.connect(('127.0.0.1',int(predPort)))
	second.send("PRED" + '/' + str(port))
	second.recv(1024)
	second.close()
	




	print "Joined"

def connect(port):
	conAdd = '127.0.0.1'+':'+ str(port)
	requestId = hashlib.sha1(conAdd).hexdigest()
	requestId = int(requestId,16)
	requestId = requestId%constant
	requested = lookup(requestId,True)
	return requested

def SUCC(port):
	global suc
	global pred
	conAdd = '127.0.0.1'+':'+ str(port)
	requestId = hashlib.sha1(conAdd).hexdigest()
	requestId = int(requestId,16)
	requestId = requestId%constant
	predecessor = str (pred[0]) + '/' + pred[1]
	pred = (int(requestId), conAdd)
	return predecessor
def PRED(port):
	global suc
	global pred
	conAdd = '127.0.0.1'+':'+ str(port)
	requestId = hashlib.sha1(conAdd).hexdigest()
	requestId = int(requestId,16)
	requestId = requestId%constant
	suc = (int(requestId), conAdd)



def helpBois(client, add):
	while True:
		try:
			code = client.recv(1024)
		except:
			break
		if "LOOK" in code:
			lookID = code.split('/')[1]
			node = lookup(int(lookID),True)
			client.send(node)
		elif "CONN" in code:
			requested = connect(int(code.split('/')[1]))
			client.send(requested)
		elif "SUCC" in code:
			predecessor = SUCC(int(code.split('/')[1]))
			client.send(predecessor)
		elif "PRED" in code:
			PRED(int(code.split('/')[1]))
			client.send("ok")
		elif "UPLOAD" in code:
			uploader(client)
		elif "DOWNLOAD" in code:
			downloader(client)



def Main():

	s = socket.socket()
	global port
	port  = sys.argv[1]
	s.bind(('127.0.0.1', int(port)))
	global address
	address = '127.0.0.1' + ':' +port
	print address
	s.listen(1000)
	global ID
	ID = hashlib.sha1(address).hexdigest()
	ID = int(ID, 16)

	ID = ID%constant
	global suc
	global pred
	suc=(int(ID),address)
	pred = (int(ID),address)
	thread.start_new_thread(interface,( s, ))
	while True:
		c, a = s.accept()
		thread.start_new_thread(helpBois,(c,a))


if __name__=="__main__":
	Main()

	# suc=(int(ID),address)
	# pred = (int(ID),address)
	# predPort = pred[1].split(':')[1]
	# first = socket.socket()
	# first.connect(('127.0.0.1',int(predPort)))

# def checker(checkSuc, first):
# 	while True:	
# 		if checkSuc!= pred:
# 			predPort = pred[1].split(':')[1]
# 			first1 = socket.socket()
# 			first1.connect(('127.0.0.1',int(predPort)))
# 			thread.start_new_thread(checker,(pred, first1 ))
# 			break
# 		elif ID==suc[0]:
# 			x=0
# 		else:
# 			try:
# 				first.send("Check")
# 				first.recv(1024)
# 			except:
# 				Usucc()


# first1 = socket.socket()
# 	first1.connect(('127.0.0.1',int(sucPort)))
# 	first1.send("W_succ" +'/' + predPort )
# 	sucessor1 = first1.recv(1024)
# 	suc1 = (int(sucessor1.split('/')[0]),sucessor1.split('/')[1])
# 	second2 = socket.socket()
# 	second2.connect(('127.0.0.1',int(predPort)))
# 	second2.send("W_pred" + '/' + sucPort)
# 	predecessor1= second2.recv(1024)
# 	pred1 = (int(predecessor1.split('/')[0]),predecessor1.split('/')[1])



	# elif "W_succ" in code :
	# 	Wsucc(int(code.split('/')[1]))
	# 	client.send(str(suc[0]) + '/' + suc[1])
	# elif "W_pred" in code :
	# 	Wpred(int(code.split('/')[1]))
	# 	client.send(str(pred[0]) + '/' + pred[1])
# def Wsucc(port):
# 	global pred1
# 	conAdd = '127.0.0.1'+':'+ str(port)
# 	requestId = hashlib.sha1(conAdd).hexdigest()
# 	requestId = int(requestId,16)
# 	requestId = requestId%constant
# 	pred1 = (int(requestId), conAdd)
# def Wpred(port):
# 	global suc1
# 	conAdd = '127.0.0.1'+':'+ str(port)
# 	requestId = hashlib.sha1(conAdd).hexdigest()
# 	requestId = int(requestId,16)
# 	requestId = requestId%constant
# 	suc1 = (int(requestId), conAdd)







# def Usucc():
# 	global pred
# 	global pred1
# 	sucPort = suc[1].split(':')[1]
# 	pred = pred1
# 	predPort = pred[1].split(':')[1]
# 	second = socket.socket()
# 	second.connect(('127.0.0.1',int(predPort)))
# 	second.send("PRED" + '/' + str(port))
# 	second.recv(1024)
# 	second.close()
# 	second2 = socket.socket()
# 	second2.connect(('127.0.0.1',int(predPort)))
# 	second2.send("Wpred" + '/' + sucPort)
# 	predecessor1= second2.recv(1024)
# 	pred1 = (int(predecessor1.split('/')[0]),predecessor1.split('/')[1])
# 	sucPort = suc[1].split(':')[1]
# 	predPort = pred[1].split(':')[1]
# 	first1 = socket.socket()
# 	first1.connect(('127.0.0.1',int(sucPort)))
# 	first1.send("W_succ" +'/' + predPort )
# 	sucessor1 = first1.recv(1024)
# def Upred():
# 	global suc
# 	global suc1
# 	sucPort = suc[1].split(':')[1]
# 	predPort = pred[1].split(':')[1]
# 	suc = suc1
# 	sucPort = suc[1].split(':')[1]
# 	first = socket.socket()
# 	first.connect(('127.0.0.1',int(sucPort)))
# 	first.send("SUCC" + '/' + str(port))
# 	predecessor = first.recv(1024)
# 	first.close()
# 	first1 = socket.socket()
# 	first1.connect(('127.0.0.1',int(sucPort)))
# 	first1.send("W_succ" +'/' + predPort )
# 	sucessor1 = first1.recv(1024)
# 	suc1 = (int(sucessor1.split('/')[0]),sucessor1.split('/')[1])
# 	sucPort = suc[1].split(':')[1]
# 	predPort = pred[1].split(':')[1]
# 	second2 = socket.socket()
# 	second2.connect(('127.0.0.1',int(predPort)))
# 	second2.send("W_pred" + '/' + sucPort)
# 	predecessor1= second2.recv(1024)