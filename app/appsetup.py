from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import scoped_session, sessionmaker

from configs import ConfigAsuToBio, ConfigLogging
import logging
import logging.config

logger = logging.getLogger(__name__)

class LoggingSetup(object):
	"""
		Logging Setup class to manage the applications logging stream.
		The logging method is an config.ini file method.  Setting the
		logging level will be pre configured for where the applciation
		is running.
	"""
	def __init__( self ):
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
		"""
			The app setup class has state, once the server configurations are all set, the first
			use of this class will be used to initate the database, there after it will be used
			just for the etl process.
		"""
		if appScope == 'asutobio':
			self.setupAsuToBio()
		elif appScope == 'DbInit':
			self.setupDbInit()
		else:
			raise TypeError('Applications scope not correctly defined, no connections setup!')
	
	def getOracleEngine( self, oracleUser, oraclePw, netServiceName ):
		"""The returned object will be an sa engine."""
		engineString = 'oracle+cx_oracle://%s:%s@%s' % ( oracleUser, oraclePw, netServiceName )
		try:
			# engine = create_engine( engineString, echo=True )
			engine = create_engine( engineString )
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

	def setupDbInit( self ):
		"""
			The database initalization will construct the current database the application is
			sourced too, from that point on the database will be up or down versioned using
			the alembic migration DB and code workflow.
		"""
		config = ConfigAsuToBio()
		config.setDbTarget()

		self.targetEngine = self.getMysqlEngine(
												config.targetUser,
												config.targetUserPw,
												config.targetDbHost,
												config.targetDbName )

	def setupAsuToBio( self ):
		"""
			When the application scope is set appropriately we then setup the object accordingly
			Sets the target and source session factories, to be referenced when a session is 
			required.  The sessions are scoped so they should all be equivelent.

			In addition to the sessions, when this scope is set, an ssh tunnel is required to 
			reach the source database.  The sub process is run seperate from the python script,
			so there could be issues cleaning up the tunnel(s)... Expand as required...

		"""		
		config = ConfigAsuToBio()

		config.setSshTunnel()

		try:
			from app.sshtunnels import SshTunnels

			self.sshtunnel = SshTunnels()

			self.sshtunnel.createSshTunnel(
								config.localport,
								config.oracleServer,
								config.oraclePort,
								config.sshUser,
								config.proxyServer )

		except OSError as e:
			print "Error trying to create the ssh tunnel!"
			raise e
		else:
			try:
				from models.biopublicmodels import BioPublic
				from models.asudwpsmodels import AsuDwPs
				
				config.setDbSource()

				sourceEngine = self.getOracleEngine(
					config.sourceUser,
					config.sourceUserPw,
					config.sourceNetServiceName )

				config.setDbTarget()

				targetEngine = self.getMysqlEngine(
					config.targetUser,
					config.targetUserPw,
					config.targetDbHost,
					config.targetDbName )


				AsuDwPs.metadata.bind = sourceEngine
				BioPublic.metadata.bind = targetEngine

				self.SrcSession = scoped_session( sessionmaker( bind=sourceEngine ) )
				self.TgtSession = scoped_session( sessionmaker( bind=targetEngine ) )
			except Exception as e:
				self.cleanUp()
				raise e

			# try:
			# 	# This should get factored out later
			# 	# BioPublic.metadata.drop_all( targetEngine )
			# 	BioPublic.metadata.create_all( targetEngine )

			# except Exception as e:
			# 	self.cleanUp()
			# 	raise e

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
		errors = []

		try:
			self.SrcSession.close_all()
		except Exception as e: 
			errors.append( e )

		try:
			self.TgtSession.close_all()
		except Exception as e: 
			errors.append( e )

		try:
			self.sshtunnel.closeSshTunnels()
		except OSError:
			errors.append( e )
			raise errors


