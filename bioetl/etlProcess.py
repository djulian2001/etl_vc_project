import logging
from etlRun import EtlRun
from bioBiRun import BioBiRun

logger = logging.getLogger( __name__ )

class EtlProcess( object ):
	"""
		This class will manage the high level data processing, calling on the ProcessRun
		module to access the data controller modules
		@AppSetup - a class object that can create the sessions

	"""
	def __init__( self, appSetup ):
		# super( EtlProcess,self ).__init__()
		self.sesSource = appSetup.getSourceSession()
		self.sesTarget = appSetup.getTargetSession()

	def runProcesses(self):
		"""
			The controller for the applications processes.
			Using the EtlRun class and BioBiRun class
		"""
		logger.info('ETL process run, subquery select mode: BEGINNING')
		try:
			runSubQ_mode = EtlRun( self.sesSource, self.sesTarget )
			runSubQ_mode.run()
			logger.info('ETL process run, subquery select mode: COMPLETED')

		except Exception as e:
			raise e
		

		if runSubQ_mode.getMissingEmplid():
			logger.info('Subquery select mode:  discovered {} missing emplids.'.format( len( runSubQ_mode.getMissingEmplid() ) ) )
			logger.info('ETL process run, list select mode: BEGINNING')
			try:
				runList_mode = EtlRun( self.sesSource, self.sesTarget, runSubQ_mode.getMissingEmplid() )
				runList_mode.run()
				logger.info('ETL process run, list select mode: COMPLETED')
			except Exception as e:
				raise e
			
			for emplid in runList_mode.getMissingEmplid():
				"""There should NOT be a reason as to why we had a missing value here log it and move forward."""
				logger.warning( "Discovered missing emplid:  {}".format( emplid ) )

		else:
			logger.info('Subquery select mode:  NO missing emplids discovered.')

		logger.info('BI process run: BEGINNING')
		try:
			runBi = BioBiRun( self.sesTarget )
			runBi.run_bi_x()
			logger.info('BI process run: COMPLETED')
		except Exception as e:
			raise e


