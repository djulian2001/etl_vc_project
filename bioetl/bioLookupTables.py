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
		subAffiliations = ModuleProcessController( subAffiliationProcessing, sesTarget )
		subAffiliations.processSource()
		subAffiliations.cleanTarget()

		departments = ModuleProcessController( departmentProcessing, sesTarget, sesSource )
		departments.processSource()
		departments.cleanTarget()

		jobCodes = ModuleProcessController( jobProcessing, sesTarget, sesSource )
		jobCodes.processSource()
		jobCodes.cleanTarget()
