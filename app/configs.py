import os

try:
	import ConfigParser as configparser
except ImportError:
	import configparser

class ConfigAsuToBio( object ):
	"""Setup the config settings required. Read in the config settings for the docstring for Config"""
	
	def __init__( self ):
		try:
			self.configFile = os.path.abspath( '/usr/local/etc/asutobiodesign/.asutobio_config.cfg' )
		except Exception:
			self.configFile = os.path.join( os.path.dirname(__file__), '.asutobio_config.cfg' )

	def setSshTunnel( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFile )
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
			parser.read( self.configFile )
		except Exception as e:
			raise e

		self.sourceUser				= parser.get( 'oracleDb', 'dbUser' )
		self.sourceUserPw			= parser.get( 'oracleDb', 'dbPw' )
		self.sourceNetServiceName 	= parser.get( 'oracleDb', 'dbNetServiceName' )

	def setDbTarget( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFile )
		except Exception as e:
			raise e
		else:
			self.targetUser 	= parser.get( 'mysqlBioPsDb', 'dbUser' )
			self.targetUserPw	= parser.get( 'mysqlBioPsDb', 'dbPw' )
			self.targetDbHost 	= parser.get( 'mysqlBioPsDb', 'dbServer' )
			self.targetDbName 	= parser.get( 'mysqlBioPsDb', 'dbName' )

class ConfigBioetl( object ):
	"""Setup the config settings required. Read in the config settings for the docstring for Config"""
	
	def __init__( self ):
		try:
			self.configFile = os.path.abspath( '/usr/local/etc/asutobiodesign/.bioetl_config.cfg' )
		except Exception:
			self.configFile = os.path.join( os.path.dirname(__file__), '.bioetl_config.cfg' )

		# print self.configFile


	def setDbTarget( self ):
		"""sets the target configsettings to be used by the application when needed"""
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFile )
		except Exception as e:
			raise e
		else:
			self.targetUser		= parser.get( 'mysqlBioPublicDb', 'dbUser' )
			self.targetUserPw	= parser.get( 'mysqlBioPublicDb', 'dbPw' )
			self.targetDbHost	= parser.get( 'mysqlBioPublicDb', 'dbServer' )
			self.targetDbName	= parser.get( 'mysqlBioPublicDb', 'dbName' )

	def setDbSource( self ):
		parser = configparser.RawConfigParser()
		try:
			parser.read( self.configFile )
		except Exception as e:
			raise e
		else:
			self.sourceUser		= parser.get( 'mysqlBioPsDb', 'dbUser' )
			self.sourceUserPw	= parser.get( 'mysqlBioPsDb', 'dbPw' )
			self.sourceDbHost	= parser.get( 'mysqlBioPsDb', 'dbServer' )
			self.sourceDbName	= parser.get( 'mysqlBioPsDb', 'dbName' )
