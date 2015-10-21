try:
	import ConfigParser as configparser
except ImportError:
	import configparser

class Config( object ):
	"""Setup the config settings required.Read in the config settings for the docstring for Config"""
	
	def __init__( self ):

		try:
			self.configFile = [ os.path.abspath( '/usr/local/etc/biodumpsapp/.config.cfg' ) ]
			self.setConfigValues()

		except Exception:
			self.configFile = [ os.path.join( os.path.dirname(__file__), '.config.cfg' ) ]
			self.setConfigValues()
			

	def setConfigValues( self ):
		parser = configparser.RawConfigParser()
		parser.read(self.configFile)

		self.ssh_user = parser.get('sshuser','user')
		self.ssh_host = parser.get('sshuser','devhost')

		self.dump_user = parser.get('dumpuser','user')
		self.dump_pw = parser.get('dumpuser','pw')

		self.restore_user = parser.get('restoreuser','user')
		self.restore_pw  = parser.get('restoreuser','pw')