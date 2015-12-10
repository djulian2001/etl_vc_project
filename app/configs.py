import os

try:
	import ConfigParser as configparser
except ImportError:
	import configparser

class ConfigAsuToBio( object ):
	"""
		Setup the config settings required. Read in the config settings for the docstring for Config
		NOTE:
		The following pulls in 2 config files, I will need to re-code this to 1 file, if and only if
		this branch works as I expect it will.
	"""
	
	def __init__( self ):
		try:
			self.configFileSrc = os.path.abspath( '/usr/local/etc/asutobiodesign/.asutobio_config.cfg' )
		except Exception:
			self.configFileSrc = os.path.join( os.path.dirname(__file__), '.asutobio_config.cfg' )

		try:
			self.configFileTgt = os.path.abspath( '/usr/local/etc/asutobiodesign/.bioetl_config.cfg' )
		except Exception:
			self.configFileTgt = os.path.join( os.path.dirname(__file__), '.bioetl_config.cfg' )


	def setSshTunnel( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFileSrc )
		except Exception as e:
			raise e

		self.sshUser 		= parser.get( 'sshTunnel', 'sshUser' )
		self.localport 		= parser.get( 'sshTunnel', 'localport' )
		self.proxyServer 	= parser.get( 'sshTunnel', 'proxyServer' )
		self.oraclePort 	= parser.get( 'sshTunnel', 'oraclePort' )
		self.oracleServer 	= parser.get( 'sshTunnel', 'oracleServer' )

	def setDbSource( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFileSrc )
		except Exception as e:
			raise e

		self.sourceUser				= parser.get( 'oracleDb', 'dbUser' )
		self.sourceUserPw			= parser.get( 'oracleDb', 'dbPw' )
		self.sourceNetServiceName 	= parser.get( 'oracleDb', 'dbNetServiceName' )

	def setDbTarget( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFileTgt )
		except Exception as e:
			raise e
		else:
			self.targetUser		= parser.get( 'mysqlBioPublicDb', 'dbUser' )
			self.targetUserPw	= parser.get( 'mysqlBioPublicDb', 'dbPw' )
			self.targetDbHost	= parser.get( 'mysqlBioPublicDb', 'dbServer' )
			self.targetDbName	= parser.get( 'mysqlBioPublicDb', 'dbName' )
