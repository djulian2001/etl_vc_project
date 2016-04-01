from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import scoped_session, sessionmaker

from configs import ConfigAsuToBio, ConfigLogging
import logging
import logging.config

class LoggingSetup(object):
	"""
		Logging Setup class to manage the applications logging stream.
		The logging method is an config.ini file method.  Setting the
		logging level will be pre configured for where the applciation
		is running.
	"""
	def __init__( self ):
		super(LoggingSetup, self).__init__()
		self.logConfig = ConfigLogging()
		
		logging.config.fileConfig( self.logConfig.getConfigFile(), disable_existing_loggers = False )


class AppSetup(object):
	"""
		Application Setup and manager of the external resources.
		 	- database connections, creation and closing of the resource
		 	- ssh tunnel, creation and close of the tunnel
		 	- sqlalchemy session factories, creation and closing of the resource
	 """
	def __init__( self, appScope ):
		# how to tie in the logging of the application setup?
		# logger = logging.getLogger(__name__)
		
		self.appScope = appScope

		if self.appScope == 'asutobio':
			self.config = ConfigAsuToBio()
			self.setupAsuToBio()
		else:
			raise TypeError('Applications scope not correctly defined, no connections setup!')
	
	def getOracleEngine( self, oracleUser, oraclePw, netServiceName ):
		"""The returned object will be an sa engine."""
		engineString = 'oracle+cx_oracle://%s:%s@%s' % ( oracleUser, oraclePw, netServiceName )
		try:
			engine = create_engine( engineString, echo=True )
		except DBAPIError:
			raise
		
		return engine

	def getMysqlEngine( self, dbUser, dbPw, dbHost, dbName ):
		"""
			Creates and returns an sqlalchemy engine to a mysql database when passed in the appropriate
			connection string settings.
			@Return: an sqlalchemy mysql+mysqldb engine
		"""
		engineString = 'mysql+mysqldb://%s:%s@%s/%s' % ( dbUser, dbPw, dbHost, dbName )

		try:
			# engine = create_engine( engineString, echo=True )
			engine = create_engine( engineString )
		except DBAPIError:
			raise

		return engine

	def setupAsuToBio( self ):
		"""
			When the application scope is set appropriately we then setup the object accordingly
			Sets the target and source session factories, to be referenced when a session is 
			required.  The sessions are scoped so they should all be equivelent.

			In addition to the sessions, when this scope is set, an ssh tunnel is required to 
			reach the source database.  The sub process is run seperate from the python script,
			so there could be issues cleaning up the tunnel(s)... Expand as required...

		"""

		self.config.setSshTunnel()

		try:
			from app.sshtunnels import SshTunnels

			self.sshtunnel = SshTunnels()

			self.sshtunnel.createSshTunnel(
				self.config.localport,
				self.config.oracleServer,
				self.config.oraclePort,
				self.config.sshUser,
				self.config.proxyServer )

		except OSError as e:
			print "Error trying to create the ssh tunnel!"
			raise e
		else:
			from bioetl.models.biopublicmodels import BioPublic
			from bioetl.models.asudwpsmodels import AsuDwPs
			
			self.config.setDbSource()

			sourceEngine = self.getOracleEngine(
				self.config.sourceUser,
				self.config.sourceUserPw,
				self.config.sourceNetServiceName )

			self.config.setDbTarget()

			targetEngine = self.getMysqlEngine(
				self.config.targetUser,
				self.config.targetUserPw,
				self.config.targetDbHost,
				self.config.targetDbName )


			AsuDwPs.metadata.bind = sourceEngine
			BioPublic.metadata.bind = targetEngine

			self.SrcSession = scoped_session( sessionmaker( bind=sourceEngine ) )
			self.TgtSession = scoped_session( sessionmaker( bind=targetEngine ) )

			try:
				# This should get factored out later
				# BioPublic.metadata.drop_all( targetEngine )
				BioPublic.metadata.create_all( targetEngine )

			except Exception as e:
				self.cleanUp()
				raise e

	def getTargetSession( self ):
		"""This method returns a configured session scoped as a target database"""
		sesTarget = self.TgtSession()
		return sesTarget
	
	def getSourceSession( self ):
		"""This method returns a configured session bound to a source database"""
		sesSource = self.SrcSession()
		return sesSource

	def cleanUp( self ):
		"""Close all session, engines, tunnels, etc..."""
		self.SrcSession.close()
		self.TgtSession.close()
		
		if self.appScope == 'asutobio':
			try:
				self.sshtunnel.closeSshTunnels()
			except OSError:
				raise


