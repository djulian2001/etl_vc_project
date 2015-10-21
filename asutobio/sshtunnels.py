import shlex
import subprocess
import time

class SshTunnels(object):
	""" SshTunnels class:
		
		Create and close an sshTunnel to a remote database server.  The tunnel 
		has to been open and functional for our side to connect and work with 
		the database server.  We pivot off a sever that has fire wall access

		System Admin Requirements:
			RSA key for the user has been established on pivot/proxy server
			All libraries that are required to run ssh on the localhost server 
				are installed
	"""
	
	def __init__(self):
		"""	
			Initialize the class
			@processList: a list that keeps track of 'all' of the tunnels open
			@processListIds: a list of all the process ids created by this class
		"""
		self.processList = []
		self.processListIds = []

	def createSshTunnel(self, localport, dbServer, dbPort, user, server):
		"""
			Create an ssh tunnel to the dbServer (the database connection server).
			The tunnel requires an RSA key be in place.
		"""

		# works because the rsa public key was set on server host.
		sshTunnelCmd = "ssh -N -L%s:%s:%s %s@%s" % (
			localport, dbServer, dbPort, user, server)

		# create the subprocess (aka the ssh tunnel)
		sshArgs = shlex.split(sshTunnelCmd)
		sshTunnel = subprocess.Popen(sshArgs)
		
		# LET THE SSH TUNNEL subprocess cmd OPEN BEFORE MOVING ON with the script...
		time.sleep(2)

		# save the process details for closing time
		self.processListIds.append(sshTunnel.pid)
		self.processList.append(sshTunnel)


	def closeSshTunnels(self):
		"""
			The method closes the list of processes appended to the attribute 
			processList, the ssh tunnel process, created by the processList class
		"""
		for sshTunnel in self.processList:
		 	try:
		 		# call the subprocess and kill it, bye bye
		 		sshTunnel.kill()
		 	except OSError:
		 		pass


# POTENTIAL Future way to clean up left over processes that have to be dealt with because of un-closed tunnels.
# from subprocess import check_output
# import shlex

# def kill_running_sshtunnel_on_port(hostname_ip, portnumber):
# 	cmdNetstat = "netstat -lpnt | grep %s:%s" % (hostname_ip, portnumber)
# 	args = shlex.split(cmdNetstat)
# 	return check_output(args)


# localhost_ip = "127.0.0.1"
# localport    = "1521"

# print kill_running_sshtunnel_on_port(localhost_ip, localport)