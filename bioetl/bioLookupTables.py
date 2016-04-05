from moduleProcessController import ModuleProcessController

import subAffiliationProcessing
import departmentProcessing
import jobProcessing

class BioLookupTables( object ):
	"""
		BioLookupTables sets the reference tables that will resolve the foreign key
		constraints within the target database.
	"""
	def __init__( self, sesSource, sesTarget ):
		subAffiliations = ModuleProcessController( subAffiliationProcessing, sesTarget )
		subAffiliations.runControllerModule()

		departments = ModuleProcessController( departmentProcessing, sesTarget, sesSource )
		departments.runControllerModule()

		jobCodes = ModuleProcessController( jobProcessing, sesTarget, sesSource )
		jobCodes.runControllerModule()
