import socket
import sys

"""
Client:
	Num: 	mulitple clients;
	I/O: 	only communicate with given-follower;
			provide user interfaces of preempt/release/check lock;
			sent lock request to follower;	 

"""

follower_socket = None
client_id = 0


def connect_follower(host, port):     
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect((host, port))
	s.sendall(b"NewClient")
	data = s.recv(1024).split(":")
	#print data
	if data[0] == "ClientID":
		return s, int(data[1])
	return None, None


def preempt_lock(lock):
	follower_socket.sendall(b"PreemptLock:%s" % lock)
	data = follower_socket.recv(1024).split(":")
	print data
	if data[0] == "Failed":
		return "Failed"
	else:
		return "Success"


def release_lock(lock):
	follower_socket.sendall(b"ReleaseLock:%s" % lock)
	data = follower_socket.recv(1024).split(":")
	print data
	if data[0] == "Failed":
		return "Failed"
	else:
		return "Success"


def check_lock(lock):
	follower_socket.sendall(b"CheckLock:%s" % lock)
	data = follower_socket.recv(1024).split(":")
	#print data
	return data


if __name__ == '__main__':
	if len(sys.argv) < 2:
		print "usage: follower_port\n"
		sys.exit(1)

	host = socket.gethostname()    
	follower_port = int(sys.argv[1])

	# connect to leader
	follower_socket, client_id = connect_follower(host, follower_port)
	print "Follower connected, get clientID: %s" % client_id


	while True:
		cmd = raw_input("Client%d: " %client_id).split(" ")
		#print cmd

		if cmd[0] == "preempt":
			if cmd[1] == None:
				print "Wrong command! Try again."
				continue
			ret = preempt_lock(cmd[1])

		elif cmd[0] == "release":
			if cmd[1] == None:
				print "Wrong command! Try again."
				continue
			ret = release_lock(cmd[1])

		elif cmd[0] == "check":
			if cmd[1] == None:
				print "Wrong command! Try again."
				continue
			ret = check_lock(cmd[1])
			if ret[0] == "True":
				print "Result: lockName: %s, ownerID: %s" % (cmd[1], ret[1])
			else:
				print "Result: lockName: %s, ownerID: none" % (cmd[1])

		else:
			print "Wrong command! Try again."


		


