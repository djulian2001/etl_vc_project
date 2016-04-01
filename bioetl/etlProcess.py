import logging

logger = logging.getLogger( __name__ )

class EtlProcess( object ):
	"""
		This class will manage the data processing modules to move data from the source session,
		to the target session. 

	"""
	def __init__( self, sesTarget, sesSource ):
		super(EtlProcess, self).__init__()
		self.missingemplid = []
		self.sesTarget = sesTarget
		self.sesSource = sesSource

		logger.info("in the init method...")
	
	# interface()?


	# method runs.