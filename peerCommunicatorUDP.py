from socket  import *
from constMP import * #-
import threading
import random
import time
import pickle
from requests import get
import os
from clientNameServer import *

#handShakes = [] # not used; only if we need to check whose handshake is missing

# Counter to make sure we have received handshakes from all other processes
handShakeCount = 0

PEERS = []

# UDP sockets to send and receive data messages:
# Create send socket
sendSocket = socket(AF_INET, SOCK_DGRAM)
#Create and bind receive socket
recvSocket = socket(AF_INET, SOCK_DGRAM)
# Allow immediate reuse of the port after closing
recvSocket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

# TCP socket to receive start signal from the comparison server:
serverSock = socket(AF_INET, SOCK_STREAM)
# Allow immediate reuse of the port after closing
serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def msg_command(msg):
	txt = msg.split(':')
	#if txt[0] == "Escreva" and int(txt[2]) == 1:
	#	with open(filename, 'r', ) as f:
	#		linhas = f.readlines()
	#	# 2. Modifica a linha específica (se a linha existir)
	#	if 0 <= int(txt[2]) < len(linhas):
	#		linhas[int(txt[2])] = str(txt[1])
	#	with open(filename, 'w') as f:
	#		f.writelines(linhas)
	#		f.flush() # Garante que o dado saia do buffer para o disco
	#	print(f"Registrado no arquivo: " + str(txt[1]) + ", na linha: " + str(txt[2]))
	#	#print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
	if not os.path.exists(filename):
		with open(filename, 'w') as f:
			pass  # Apenas cria o arquivo vazio
			print(f"Arquivo '{filename}' criado com sucesso!")	
			
	if txt[0] == "Escreva" and int(txt[2]) >= 1:
		with open(filename, 'r') as f:
			linhas = f.readlines()
		# 2. Modifica a linha específica (se a linha existir)
		if 0 <= int(txt[2]) < len(linhas):
			linhas[int(txt[2])-1] = str(txt[1])
		else:
			while len(linhas) <= int(txt[2]):
				linhas.append('\n')
			linhas[int(txt[2])-1] = str(txt[1])
			print('Número de linha fora do intervalo.')
		# 3. Reescreve o arquivo com a alteração
		with open(filename, 'w') as f:
			f.writelines(linhas)
			f.flush()
		print(f"Registrado no arquivo: " + str(txt[1]) + ", na linha: " + str(txt[2]))
		
		 
def get_public_ip():
  ipAddr = PEER1_ADDR					#get('https://api.ipify.org').content.decode('utf8')
  print('My public IP address is: {}'.format(ipAddr))
  return ipAddr

# Function to register this peer with the group manager
def registerWithGroupManager():
  clientSock = socket(AF_INET, SOCK_STREAM)
  print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  ipAddr = get_public_ip()
  req = {"op":"register", "ipaddr":ipAddr, "port":PEER_UDP_PORT}
  msg = pickle.dumps(req)
  print ('Registering with group manager: ', req)
  clientSock.send(msg)
  clientSock.close()

def getListOfPeers():
  client = NameServiceClient()
  PEERS = client.discover("peer")
  enderecos = [p["endereco"] for p in PEERS["processos"]]
  print(enderecos)
  print(PEERS)
  #clientSock = socket(AF_INET, SOCK_STREAM)
  #print ('Connecting to group manager: ', (GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  #clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
  #req = {"op":"list"}
  #msg = pickle.dumps(req)
  #print ('Getting list of peers from group manager: ', req)
  #clientSock.send(msg)
  #msg = clientSock.recv(2048)
  #PEERS = pickle.loads(msg)
  #print ('Got list of peers: ', PEERS)
  #clientSock.close()
  return enderecos

class MsgHandler(threading.Thread):
  def __init__(self, sock):
    threading.Thread.__init__(self)
    self.sock = sock

  def run(self):
    print('Handler is ready. Waiting for the handshakes...')
    
    #global handShakes
    global handShakeCount
    
    logList = []
    
    # Wait until handshakes are received from all other processes
    # (to make sure that all processes are synchronized before they start exchanging messages)
    while handShakeCount < N:
      msgPack = self.sock.recv(1024)
      msg = pickle.loads(msgPack)
      #print ('########## unpickled msgPack: ', msg)
      if msg[0] == 'READY':

        # To do: send reply of handshake and wait for confirmation

        handShakeCount = handShakeCount + 1
        #handShakes[msg[1]] = 1
        print('--- Handshake received: ', msg[1])

    print('Secondary Thread: Received all handshakes. Entering the loop to receive messages.')

	#recebe a mensagem e escreve no arquivo
    stopCount=0 
    while True:                
      msgPack = self.sock.recv(1024)   # receive data from client
      msg = pickle.loads(msgPack)
      if msg[0] == -1:   # count the 'stop' messages from the other processes
        stopCount = stopCount + 1
        if stopCount == N:
          break  # stop loop when all other processes have finished
      else:
      	msg_command(str(msg[1]))
      	#with open(filename, 'a') as f:
      	#	f.write(str(msg[1]))
      	#	f.flush() # Garante que o dado saia do buffer para o disco]
      	#print(f"Registrado no arquivo: " + str(msg[1]))	
      	#print('Message ' + str(msg[1]) + ' from process ' + str(msg[0]))
      	logList.append(msg)
        
    # Write log file
    logFile = open('logfile'+str(myself)+'.log', 'w')
    logFile.writelines(str(logList))
    logFile.close()
    
    # Send the list of messages to the server (using a TCP socket) for comparison
    print('Sending the list of messages to the server for comparison...')
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((SERVER_ADDR, SERVER_PORT))
    msgPack = pickle.dumps(logList)
    clientSock.send(msgPack)
    clientSock.close()
    
    # Reset the handshake counter
    handShakeCount = 0

    exit(0)

# Function to wait for start signal from comparison server:
def waitToStart():
  (conn, addr) = serverSock.accept()
  msgPack = conn.recv(1024)
  msg = pickle.loads(msgPack)
  myself = msg[0]
  nMsgs = msg[1]
  conn.send(pickle.dumps('Peer process '+str(myself)+' started.'))
  conn.close()
  return (myself,nMsgs)

# From here, code is executed when program starts:
#registerWithGroupManager()
client = NameServiceClient()
print(client.bind("peer1", "tcp://192.168.40.194:5679"))
#print(client.bind("peer2", "tcp://192.168.1.81:5679"))
#print(client.bind("peer3", "tcp://192.168.1.81:5679"))
print(client.register("peer1", "peer"))
#print(client.register("peer2", "peer"))
#print(client.register("peer3", "peer"))
print(client.discover("peer"))
print(client.lookup("peer1"))
while 1:
  print('Waiting for signal to start...')
  (myself, nMsgs) = waitToStart()
  print('I am up, and my ID is: ', str(myself))

  if nMsgs == 0:
    print('Terminating.')
    exit(0)

  # Create receiving message handler
  msgHandler = MsgHandler(recvSocket)
  msgHandler.start()
  print('Handler started')

  PEERS = getListOfPeers()
  
  # Send handshakes
  # To do: Must continue sending until it gets a reply from each process
  #        Send confirmation of reply
  for addrToSend in PEERS:
  	limpo = addrToSend.replace("tcp://", "") # Fica: '192.168.1.81:5679'
  	print("l: " + limpo)
  	ip, porta = limpo.split(":")
  	print('Sending handshake to ', addrToSend)
  	msg = ('READY', myself)
  	msgPack = pickle.dumps(msg)
  	sendSocket.sendto(msgPack, (ip,PEER_UDP_PORT))
    #data = recvSocket.recvfrom(128) # Handshadke confirmations have not yet been implemented

  print('Main Thread: Sent all handshakes. handShakeCount=', str(handShakeCount))

  while (handShakeCount < N):
    pass  # find a better way to wait for the handshakes

  # Send a sequence of data messages to all other processes 
  for msgNumber in range(0, nMsgs):
    # Wait some random time between successive messages
    time.sleep(random.randrange(10,100)/1000)
    msg = (myself, "Escreva:Voce esta perdendo os lembretes do Duo :2")#msg = (myself, msgNumber)
    msgPack = pickle.dumps(msg)
    for addrToSend in PEERS:
      limpo = addrToSend.replace("tcp://", "") # Fica: '192.168.1.81:5679'
      print("l: " + limpo)
      ip, porta = limpo.split(":")
      sendSocket.sendto(msgPack, (ip,PEER_UDP_PORT))
      print('Sent message ' + str(msgNumber))

  # Tell all processes that I have no more messages to send
  for addrToSend in PEERS:
    limpo = addrToSend.replace("tcp://", "") # Fica: '192.168.1.81:5679'
    print("l: " + limpo)
    ip, porta = limpo.split(":")
    msg = (-1,-1)
    msgPack = pickle.dumps(msg)
    sendSocket.sendto(msgPack, (ip,PEER_UDP_PORT))
