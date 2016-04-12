
# NOTE MIGHT NOT NEED THIS.....
import logging

logger = logging.getLogger( __name__ )

class ValidateSourceDataEmplids(object):
	"""docstring for ValidateSourceDataEmplids"""
	def __init__(self, sesSource, emplids):
		self.sesSource = sesSource
		self.emplids = emplids
		self.validEmplids = []

	def runMe(self):
		"""Run the data validation process..."""
		from processControllers import personProcessing
