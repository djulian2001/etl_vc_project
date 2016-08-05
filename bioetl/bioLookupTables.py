from moduleProcessController import ModuleProcessController

from processControllers import subAffiliationProcessing
from processControllers import departmentProcessing
from processControllers import jobProcessing

class BioLookupTables( object ):
	"""
		BioLookupTables sets the reference tables that will resolve the foreign key
		constraints within the target database.  All of these tables have a need to
		processed prior to walking through the personnel data.  A contract.
	"""
	def __init__( self, sesSource, sesTarget ):
		self.sesTarget = sesTarget
		self.sesSource = sesSource

	def runMe( self ):
		"""Run the processes against the bio lookup tables"""
		subAffiliations = ModuleProcessController( subAffiliationProcessing, self.sesTarget )
		# Because in this case this instances doesn't have the normal source database
		subAffiliations.sesSource = self.sesTarget
		subAffiliations.processSource()
		subAffiliations.cleanTarget()

		departments = ModuleProcessController( departmentProcessing, self.sesTarget, self.sesSource )
		departments.processSource()
		departments.cleanTarget()

		jobCodes = ModuleProcessController( jobProcessing, self.sesTarget, self.sesSource )
		jobCodes.processSource()
		jobCodes.cleanTarget()
