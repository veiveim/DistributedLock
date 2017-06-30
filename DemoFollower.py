import thread
import socket
import sys
import Queue

"""
Follower:
	Num: 	multiple followers;
	I/O: 	communicate with leader & clients;
		 	assign clientID to clients;
		 	relay preempt/release request/response between leader & clients;
			response to check request of clients;
"""

clients = []
locks = []
req_msgs = []



def connect_leader(leader_host, leader_port):     
	leader_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	leader_socket.connect((leader_host, leader_port))
	leader_socket.sendall(b"NewFollower")
	data = leader_socket.recv(1024).split(":")
	
	if data[0] == "FollowID":
		follwer_id = int(data[1])

	index = 0
	while True:
		leader_socket.sendall(b"UpdateMap:%d" % index)
		msg = leader_socket.recv(1024).split(":")
		#print msg
		if msg[0] == "UpdateMap" and msg[1] != "Failed":
			index += 1
			locks.append({'name': msg[1], 'client': msg[2]})
		else:
			break

	return leader_socket, follwer_id


# relay leader's msg to client
def downstream_thread(leader_socket):
	while True:
		data = leader_socket.recv(1024)
		#print "data: ", data
		if not data:
			continue

		msg = data.split(":") 
		#print "msg: ", msg
		if msg[0] == "PreemptLock":
			locks.append({'name': msg[1], 'client': msg[2]})
		elif msg[0] == "ReleaseLock":
			locks.remove({'name': msg[1], 'client': msg[2]})

		if len(req_msgs) == 0:
			continue

		req = req_msgs[0].split(":")
		#print "req: ", req, msg == req

		if msg[0] == "Failed":
			c = get_client_conn(req[2])
			if not c:
				print "no such client:", req[2]
				continue
			c.sendall("%s:Failed" % req)
			req_msgs.pop(0)

		elif msg == req:
			#print "request success"
			c = get_client_conn(req[2])
			if not c:
				print "no such client:", req[2]
				continue
			c.sendall("%s:Success" % data)	
			req_msgs.pop(0)


def new_client(conn):
	client_id = follower_id * 10000 + len(clients)
	clients.append(
		{'id': client_id, 'conn': conn})
	return client_id


def get_client_conn(client_id):
	for client in clients:
		#print client['id']
		if client['id'] == int(client_id):
			return client['conn']
	return None


def check_lock(lock):
	for l in locks:
		#print l['name'], l['client']
		if l['name'] == lock:
			return {'result': True, 'client': l['client']}
	
	return {'result': False}


#  relay clients' msg to leader
def upstream_thread(leader_socket, c):
	client_id = 0
	while True:
		try:
			data = c.recv(1024)
			if not data: 
				continue

			print "Request: " + data
			msg = data.split(":")

			if msg[0] == "NewClient":
				client_id = new_client(c)
				c.sendall("ClientID: %d" % client_id)
				continue

			if msg[0] == "PreemptLock":
				req = "PreemptLock:%s:%d" % (msg[1], client_id)
				req_msgs.append(req)
				leader_socket.sendall(b"%s" % req)
				continue

			if msg[0] == "ReleaseLock":
				req = "ReleaseLock:%s:%d" %(msg[1], client_id)
				req_msgs.append(req)
				leader_socket.sendall(b"%s" % req)
				continue

			if msg[0] == "CheckLock":
				ret = check_lock(msg[1])
				if ret['result'] == True:
					c.sendall("True:%s" % ret['client'])
				else:
					c.sendall("False")
				continue

		except socket.error:
			print "Socket Error Occured."
			c.close()
			return 0


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print "usage: self_port leader_port\n"
		sys.exit(1)

	host = socket.gethostname()    
	self_port = int(sys.argv[1])
	leader_port = int(sys.argv[2])

	# connect to leader
	leader_socket, follower_id = connect_leader(host, leader_port)
	thread.start_new_thread(downstream_thread, (leader_socket,))
	print "Leader connected, get followerID: %d" % follower_id
   
   	# bind self socket
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((host, self_port))
	s.listen(1)


	while True:
		# accept new connection
		conn, addr = s.accept()
		print "Connected by", addr

		thread.start_new_thread(upstream_thread, (leader_socket, conn, ))
		