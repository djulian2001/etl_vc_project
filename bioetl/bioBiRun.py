import logging

logger = logging.getLogger( __name__ )

class BioBiRun( object ):
 	"""The BioBiRun is the translation portion of etl process run, this class will
 		manage the modules that will try and extract meaning and value from the 
 		source database.
 		@sesTarget - is the target database sqlalchemy session
 		@...

 		returns:
 			changes to the sesTarget data
 		"""

 	def __init__(self, sesTarget):
 		self.sesTarget = sesTarget
 		 

 	def run_bi_x(self):
 		"""Expand these as required..."""
 		pass