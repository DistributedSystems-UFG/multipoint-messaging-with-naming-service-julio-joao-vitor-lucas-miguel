from socket import *
import pickle
from constMP import *
import time
import sys
from clientNameServer import *

class comparisonServer:
	def __init__(self):
		self.serverSock = socket(AF_INET, SOCK_STREAM)
		# Allow immediate reuse of the port after closing
		self.serverSock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
		self.serverSock.bind(('0.0.0.0', SERVER_PORT))
		self.serverSock.listen(2)

	def mainLoop(self):
		cont = 1
		while 1:
			nMsgs = self.promptUser()
			client = NameServiceClient()
			client.bind("comparisonServer", "tcp://192.168.40.203:5678")
			print(client.register("comparisonServer", "server"))
			peerList = client.discover("peer")
			enderecos = [p["endereco"] for p in peerList["processos"]]
			print(enderecos)
			#clientSock = socket(AF_INET, SOCK_STREAM)
			#clientSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
			#req = {"op":"list"}
			#msg = pickle.dumps(req)
			#clientSock.send(msg)
			#msg = clientSock.recv(2048)
			#clientSock.close()
			#peerList = pickle.loads(msg)
			print("List of Peers: ", enderecos)
			self.startPeers(enderecos,nMsgs)
			if nMsgs != 0:
				print('Now, wait for the message logs from the communicating peers...')
				self.waitForLogsAndCompare(nMsgs)
			else:
				print('Stopping.')
				serverSock.close()
				
				# Tell group manager to stop
				cSock = socket(AF_INET, SOCK_STREAM)
				cSock.connect((GROUPMNGR_ADDR,GROUPMNGR_TCP_PORT))
				req = {"op":"stop"}
				msg = pickle.dumps(req)
				cSock.send(msg)
				cSock.close()
				break

	def promptUser(self):
		nMsgs = int(input('Enter the number of messages for each peer to send (0 to terminate)=> '))
		return nMsgs

	def startPeers(self,peerList,nMsgs):
		# Connect to each of the peers and send the 'initiate' signal:
		peerNumber = 0
		for peer in peerList:
			limpo = peer.replace("tcp://", "") # Fica: '192.168.1.81:5679'
			ip, porta = limpo.split(":")
			clientSock = socket(AF_INET, SOCK_STREAM)
			clientSock.connect((ip,int(porta)))
			msg = (peerNumber,nMsgs)
			msgPack = pickle.dumps(msg)
			clientSock.send(msgPack)
			msgPack = clientSock.recv(512)
			print(pickle.loads(msgPack))
			clientSock.close()
			peerNumber = peerNumber + 1

	def waitForLogsAndCompare(self,N_MSGS):
		# Loop to wait for the message logs for comparison:
		numPeers = 0
		msgs = [] # each msg is a list of tuples (with the original messages received by the peer processes)

		# Receive the logs of messages from the peer processes
		while numPeers < N:
			(conn, addr) = self.serverSock.accept()
			msgPack = conn.recv(32768)
			print ('Received log from peer')
			conn.close()
			msgs.append(pickle.loads(msgPack))
			numPeers = numPeers + 1

		unordered = 0

		# Compare the lists of messages
		for j in range(0,N_MSGS-1):
			firstMsg = msgs[0][j]
			for i in range(1,N-1):
				if firstMsg != msgs[i][j]:
					unordered = unordered + 1
					break
		
		print ('Found ' + str(unordered) + ' unordered message rounds')


# Initiate server:
comparisonServer = comparisonServer()
comparisonServer.mainLoop()
