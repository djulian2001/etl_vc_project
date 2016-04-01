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
		# parser = configparser.RawConfigParser()
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
		# parser = configparser.RawConfigParser()
		parser = configparser.SafeConfigParser()
		try:
			parser.read( self.appConfigFile )
		except Exception as e:
			raise e

		self.sourceUser				= parser.get( 'oracleDb', 'dbUser' )
		self.sourceUserPw			= parser.get( 'oracleDb', 'dbPw' )
		self.sourceNetServiceName 	= parser.get( 'oracleDb', 'dbNetServiceName' )

	def setDbTarget( self ):
		# parser = configparser.RawConfigParser()
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

		# parser = configparser.SafeConfigParser()
		

		# LOGGING = {
		# 	'version': 1,
		# 	'disable_existing_loggers': True,
		# 	'formatters':{
		# 		'consoleFormatter': {
		# 			'format': '[%(levelname)s] %(name)s: %(message)s ( %(filename)s:%(lineno)d )',
		# 			'datefmt': None, },
		# 		'rotateFileFormatter': {
		# 			'format': '[%(levelname)s] %(name)s: %(message)s ( %(asctime)s; %(filename)s:%(lineno)d )',
		# 			'datefmt': "%Y-%m-%d %H:%M:%S", }, },
		# 	'handlers':{
		# 		'consoleHandler':{
		# 			'level':'DEBUG',
		# 			'formatter':'consoleFormatter',
		# 			'class':'logging.StreamHandler', },
		# 		'rotateFileHandler':{
		# 			'level':'DEBUG',
		# 			'formatter':'rotateFileFormatter',
		# 			'class':'logging.handlers.RotatingFileHandler',
		# 			'filename':'/var/log/etl-asutobiodesign/asutobiodesign.log',
		# 			'encoding':'utf8',
		# 			'mode':'a',
		# 			'maxBytes':10485760,
		# 			'backupCount':4, }, },
		# 	'loggers':{
		# 		'':{
		# 			'handlers': ['consoleHandler','rotateFileHandler'],
		# 			'level':'DEBUG', },	}, }

		# try:
		# 	parser.read( self.appConfigFile )
			# self.logger = logging.config.fileConfig( parser, disable_existing_loggers=False )
			# self.logger = logging.config.fileConfig( self.appConfigFile, disable_existing_loggers=False )
			# self.logConfig = LOGGING
			

		# except Exception as e:
		# 	raise e
		# else:
		# 	levelDict={ "CRITICAL":logging.CRITICAL,"ERROR":logging.ERROR,"WARNING":logging.WARNING,"INFO":logging.INFO,"DEBUG":logging.DEBUG,"NOTSET":logging.NOTSET, }
		# 	streamDict={ "stdout":sys.stdout, "stderr":sys.stderr }
			
		# 	def logStream( stream ):
		# 		return streamDict[ stream ]

		# 	def logLevel( lvl ):
		# 		"""Extract the appropriate level..."""
		# 		return levelDict[ lvl ]

		# 	self.rootLoggingLevel = logLevel( parser.get( 'logger_root','level' ) )
		# 	# formatters
		# 	fileFormatter = logging.Formatter(
		# 						fmt 	= parser.get( 'formatter_rotateFileFormatter','format' ) ,
		# 						datefmt = parser.get( 'formatter_rotateFileFormatter','datefmt' ) )

		# 	consoleFormatter = logging.Formatter(
		# 						fmt 	= parser.get( 'formatter_consoleFormatter','format'), 
		# 						datefmt = None )

		# 	self.fileHandler = logging.handlers.RotatingFileHandler(
		# 						parser.get('handler_rotateFileHandler','filename'),
		# 						mode		= parser.get('handler_rotateFileHandler','mode'),
		# 						maxBytes	= parser.get('handler_rotateFileHandler','maxBytes'),
		# 						backupCount	= parser.get('handler_rotateFileHandler','backupCount'),
		# 						encoding	= parser.get('handler_rotateFileHandler','encoding'),
		# 						delay		= parser.get('handler_rotateFileHandler','delay') )

		# 	self.fileHandler.setLevel( logLevel( parser.get( 'handler_rotateFileHandler','level' ) ) )
		# 	self.fileHandler.setFormatter( fileFormatter )

			# self.fileHandler = logging.handlers.TimedRotatingFileHandler(
			# 					parser.get( 'handler_timedRotatingFileHandler','filename' ),
			# 					when 		= parser.get( 'handler_timedRotatingFileHandler','when' ),
			# 					interval 	= parser.get( 'handler_timedRotatingFileHandler','interval' ),
			# 					backupCount = parser.get( 'handler_timedRotatingFileHandler','backupCount' ),
			# 					encoding 	= parser.get( 'handler_timedRotatingFileHandler','encoding' ),
			# 					delay 		= parser.get( 'handler_timedRotatingFileHandler','delay' ) )
			# 					# utc 		= parser.get( 'handler_timedRotatingFileHandler','utc' ) )

			# self.consoleHandler = logging.StreamHandler( logStream( parser.get( 'handler_consoleHandler','stream' ) ) )

			# self.consoleHandler.setLevel( logLevel( parser.get('handler_consoleHandler','level' ) ) )

			# self.consoleHandler.setFormatter( consoleFormatter )

			# because the level has to be applied to the logger, we will pass the config through.
			# self.fileHandlerLevel =	parser.get('handler_rotateFileHandler','level')
			# self.fileHandlerLevel = parser.get( 'handler_timedRotatingFileHandler','level' )
			# self.consoleHandlerLevel = parser.get('handler_consoleHandler','level')
			
