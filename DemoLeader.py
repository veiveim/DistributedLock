import thread
import socket
import sys

"""
Leader:
	Num:	only one leader;
	I/O: 	only communicate with followers;
			assign followerID to followers;
			reponse to preempt/release request of followers;
			broadcast changes of lock-map to followers;
"""


followers = []
locks = []
connections = []


def new_follower(conn):
	follower_id = len(followers)
	followers.append(
		{'id': follower_id, 'conn': conn})
	return follower_id


def preempt_lock(lock, client):
	for l in locks:
		if l['name'] == lock:
			return {'result': False}

	locks.append({'name': lock, 'client': client})
	news = ("PreemptLock:%s:%s" % (lock, client))
	return {'result': True, 'news': news}


def release_lock(lock, client):
	for l in locks:
		if l['name'] == lock and l['client'] == client:
			locks.remove({'name': lock, 'client': client})
			news = ("ReleaseLock:%s:%s" % (lock, client))
			return {'result': True, 'news': news}

	return {'result': False}


def broadcast_news(news):
	print "boradcast:", news
	for follower in followers:
		conn = follower['conn']
		conn.sendall(news)


def get_lock_by_index(index):
	if int(index) >= len(locks):
		return {}
	return locks[int(index)]


def work_thread(c):
	follower_id = -1
	while True:
	
		data = c.recv(1024)
		if not data: 
			continue

		print "Request: " + data
		msg = data.split(":")

		if msg[0] == "NewFollower":
			follower_id = new_follower(c)
			c.sendall("FollowID:%d" % follower_id)
			continue

		if msg[0] == "UpdateMap":
			lock = get_lock_by_index(msg[1])
			if not lock:
				c.sendall("Failed")
				continue
			c.sendall("UpdateMap:%s:%s" %(lock['name'], lock['client']))
			continue

		if msg[0] == "PreemptLock":
			ret = preempt_lock(msg[1], msg[2])
			if ret['result']:
				broadcast_news(ret['news'])
			else:
				c.sendall("Failed")
			continue

		if msg[0] == "ReleaseLock":
			ret = release_lock(msg[1], msg[2])
			if ret['result']:
				broadcast_news(ret['news'])
			else:
				c.sendall("Failed")
			continue


if __name__ == '__main__':
	if len(sys.argv) < 2:
		print "usage: self_port\n"
		sys.exit(1)

	host = socket.gethostname()       
	port = int(sys.argv[1])  
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((host, port))
	s.listen(8)

	while True:
		# accept new connection
		conn, addr = s.accept()
		print "Connected by", addr

		connections.append(conn)
		thread.start_new_thread(work_thread, (conn, ))

