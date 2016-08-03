from bioLookupTables import BioLookupTables
from bioPeopleTables import BioPeopleTables
import logging

logger = logging.getLogger(__name__)

class EtlRun( object ):
	"""
		The EtlRun Classes job is to guarentee and force ordered execution of the 
		etl modules required for each etl process.
		There are 2 distinct modes this class will run in, 
			1: as a subquery select mode ( first run ), and
			2: in a list select mode ( second run ) 
		The select type is always in reference to how we get the data out of the 
		source database, the select type is determined by the emplids list, if the
		list is empty it is assumed that the mode is select by subquery, otherwise
		select by whats in the list.
		@sesSource - is the source database sqlalchemy session
		@sesTarget - is the target database sqlalchemy session
		@emplids - a list of emplids that appear missing in the target db people table

		returns: 
			exceptions
			logging
			"""
	def __init__( self, sesSource, sesTarget, emplids=[] ):
		"""
			The class takes in 3 attributes, 2 are required sqlalchemy sessions, 1 is a
			list of emplid ids, which sets the state of the process run, that will be 
			passed into the child process classes, part of the mode the application is
			running in.
			The object peopleRun will always be a part of every run, but the lookup
			tables will only be apart of a specific run (subquery mode)
			The mode is 'flagged' by the truthiness of the class attribute qryByIds value.
		"""
		self.sesSource = sesSource
		self.sesTarget = sesTarget
		self.qryByIds = emplids
		
		self.peopleRun = BioPeopleTables( self.sesSource, self.sesTarget, self.qryByIds )
		
	
	def getMissingEmplid( self ):
		"""Return the emplids stored within the class attribute"""
		return self.peopleRun.getUniqueFoundMissingIds()


	def runMe( self ):
		"""
			Each run can be triggered as a subquery run or a list run, the qryByIds is what
			determines which stat the run will be processed as.  Order matters...
			subquery mode --
				bio lookup tables
					-> people data
			list mode --
				-> people data ( subset of )
		"""
		# if first run... do this stuff, else skip.
		if not self.qryByIds:
			logger.info("Processing Data for Lookup Tables:  BEGINNING")
			try:
				bioLookupTables = BioLookupTables( self.sesSource, self.sesTarget )
				bioLookupTables.runMe()
				logger.info("Lookup Tables data processing:  COMPLETED")
			except Exception as e:
				raise e

			logger.info("Configure External Application State Dependencies: BEGINNING")
			try:
				"""This is where gobal variables used by the application are set for data warehouse emplid subquery"""
				from models.asudwpsmodels import setSubAffiliationCodesList

				setSubAffiliationCodesList( self.sesTarget )

				logger.info("Application State has been configured")
			except Exception as e:
				raise e

		logger.info("Processing Personnel Data:  BEGINNING")
		try:
			
			self.peopleRun.runMe()

			logger.info("Personnel data processing:  COMPLETED")
		except Exception as e:
			raise e
