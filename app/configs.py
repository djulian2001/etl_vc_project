import os, sys

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
			self.appConfigFile = os.path.abspath( '/usr/local/etc/asutobiodesign/.bioetl_config.cfg' )
		except Exception:
			self.appConfigFile = os.path.join( os.path.dirname(__file__), '.bioetl_config.cfg' )

	def setSshTunnel( self ):
		parser = configparser.SafeConfigParser()
		try:
			parser.read( self.appConfigFile )
		except Exception as e:
			raise e

		self.sshUser 		= parser.get( 'sshTunnel', 'sshUser' )
		self.localport 		= parser.get( 'sshTunnel', 'localport' )
		self.proxyServer 	= parser.get( 'sshTunnel', 'proxyServer' )
		self.oraclePort 	= parser.get( 'sshTunnel', 'oraclePort' )
		self.oracleServer 	= parser.get( 'sshTunnel', 'oracleServer' )

	def setDbSource( self ):
		parser = configparser.SafeConfigParser()
		try:
			parser.read( self.appConfigFile )
		except Exception as e:
			raise e

		self.sourceUser				= parser.get( 'oracleDb', 'dbUser' )
		self.sourceUserPw			= parser.get( 'oracleDb', 'dbPw' )
		self.sourceNetServiceName 	= parser.get( 'oracleDb', 'dbNetServiceName' )

	def setDbTarget( self ):
		parser = configparser.SafeConfigParser()
		try:
			parser.read( self.appConfigFile )
		except Exception as e:
			raise e
		else:
			self.targetUser		= parser.get( 'mysqlBioPublicDb', 'dbUser' )
			self.targetUserPw	= parser.get( 'mysqlBioPublicDb', 'dbPw' )
			self.targetDbHost	= parser.get( 'mysqlBioPublicDb', 'dbServer' )
			self.targetDbName	= parser.get( 'mysqlBioPublicDb', 'dbName' )

	

class ConfigLogging( object ):
	"""The first part of this application is to initiate the logging.  Have to know when stuff stops working"""
	def __init__(self):
		super( ConfigLogging, self).__init__()
		try:
			self.logConfigFile = os.path.abspath( '/usr/local/etc/asutobiodesign/.bioetl_logging_config.cfg' )
		except Exception:
			self.logConfigFile = os.path.join( os.path.dirname(__file__), '.bioetl_logging_config.cfg' )

	def getConfigFile( self ):
		"""
			There are 2 logging methods being applied to this application, where we want to be able to
				scope the log output based on the appliation scope of development.  The idea being that
				when on production we will log nothing to the console, but only to the log file.  This of
				course would be set differently for staging and dev scopes.
			handlers: 
				@ as a file rotation
				@ as a console standard output
			formatters:
				@file format (includes timestamp)
				@console format (excluldes timestamp)
			handlsers levels:
				@ passing the configured value through to logger initialization and set level method call.
		"""			 
		return self.logConfigFile
