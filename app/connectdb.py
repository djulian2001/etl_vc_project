from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

class etlconnections(object):
	"""docstring for ClassName"""
	def __init__( self, appScope ):
		self.appScope = appScope

		if self.appScope == 'asutobio':
			from configs import ConfigAsuToBio
			
			self.config = ConfigAsuToBio()
			# self.setupAsuToBio()
			pass
		elif self.appScope == 'bioetl':
			from configs import ConfigBioetl

			self.config = ConfigBioetl()
			self.setupBioetl()
			pass
		else:
			raise TypeError('Applications scope not correctly defined, no connections setup!')
	
	def getOracleEngine( self, oracleUser, oraclePw, netServiceName ):
		"""The returned object will be an sa engine."""
		engineString = 'oracle+cx_oracle://%s:%s@%s' % ( oracleUser, oraclePw, netServiceName )
		try:
			engine = create_engine( engineString, echo=True )
		except Exception as e:
			raise e
		
		return engine

	def getMysqlEngine( self, dbUser, dbPw, dbHost, dbName ):
		"""
			Creates and returns an sqlalchemy engine to a mysql database when passed in the appropriate
			connection string settings.
			@Return: an sqlalchemy mysql+mysqldb engine
		"""
		engineString = 'mysql+mysqldb://%s:%s@%s/%s' % ( dbUser, dbPw, dbHost, dbName )
		print engineString

		try:
			engine = create_engine( engineString, echo=True )
		except Exception as e:
			raise e

		return engine

	def setupBioetl( self ):
		"""
			When the application scope is set appropriately we then setup the object accordingly
			Sets the target and source session factories, to be referenced when a session is 
			required.  The sessions are scoped so they should all be equivelent.
		"""
		from asutobio.models.biopsmodels import BioPs
		from bioetl.models.biopublicmodels import BioPublic

		# Initialize the config settings required for the source db connection.
		self.config.setDbSource()

		sourceEngine = self.getMysqlEngine(
			self.config.sourceUser, 
			self.config.sourceUserPw, 
			self.config.sourceDbHost, 
			self.config.sourceDbName )
		
		self.config.setDbTarget()

		targetEngine = self.getMysqlEngine(
			self.config.targetUser,
			self.config.targetUserPw,
			self.config.targetDbHost,
			self.config.targetDbName )

		
		BioPs.metadata.bind = sourceEngine
		BioPublic.metadata.bind = targetEngine

		self.SrcSession = scoped_session( sessionmaker( bind=sourceEngine ) )
		self.TgtSession = scoped_session( sessionmaker( bind=targetEngine ) )



	def setupAsuToBio( self ):
		"""pass"""

		# sourceEngine = getOracleEngine(
		# 	self.config.
		# 	)

		# sourceDbUser = "ASU_BDI_EXTRACT_APP"
		# sourceDbPw = "np55adW_G1_Um-ii"
		# sourceDbNetServiceName = "ASUPMDEV"

		pass

	def getTargetSession( self ):
		"""This method returns a configured session scoped as a target database"""
		session = self.TgtSession()
		return session
	
	def getSourceSession( self ):
		"""This method returns a configured session bound to a source database"""
		session = self.SrcSession()
		return session

	def cleanUp( self ):
		"""Close all session, engines, tunnels, etc..."""
		self.SrcSession.close()
		self.TgtSession.close()
