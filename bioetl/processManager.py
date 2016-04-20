import datetime

from models.biopublicmodels import EtlProcessManager
from bioetl.sharedProcesses import hashThisList

class ProcessManager( object ):
	"""
		ProcessManager class tracks the run, for the front end to know whats up with the
		backend process, very high level communications, the logging is where the sys
		admins should look for specific issues.
	"""
	def __init__( self, sesManager ):
		self.sesManager = sesManager
		hashed = hashThisList( [ "An ETL process run.", datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) ] )

		self.runManager = EtlProcessManager(
					source_hash= hashed,
					created_at= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ),
					started_at= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ), )

		self.commitThis()

	def commitThis( self ):
		"""
			Method to commit the runManager object with various status updates as the application
			runs.  Takes in *args, that will be added to the runManager object by reference
		"""
		self.sesManager.add( self.runManager )
		self.sesManager.commit()


	def updateRunStatus( self, runStatus ):
		""" This is the only value that will ever really be updated during process run."""
		self.runManager.run_status = runStatus
		self.runManager.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		self.commitThis()

	def badRun( self, missingIds=None ):
		"""The run needs to report it was terminated premature, sad face. :("""
		self.runManager.updated_at 				= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		self.runManager.ended_at 			  	= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		self.runManager.ending_status 			= False
		self.runManager.emplids_not_processed 	= missingIds
		self.commitThis()
		
	def goodRun( self, missingIds=None ):
		"""The run completed safely and cleanly, happy face. :)"""
		self.runManager.run_status 				= "ETL process ended cleanly"
		self.runManager.updated_at 				= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		self.runManager.ended_at 			  	= datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		self.runManager.ending_status 			= True
		self.runManager.emplids_not_processed 	= missingIds
		self.commitThis()


