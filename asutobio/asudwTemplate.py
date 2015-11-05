from models.asudwpsmodels import AsuDwPs_X_, AsuPsBioFilters
from models.biopsmodels import BioPs_Y_

from sharedProcesses import hashThisList


# the data pull
def getSource_X_Data( sesSource ):
	"""
		Selects the data from the data wharehouse for the _X_ model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return 

# the data load
def process_Y_Data():
	"""
		Process an AsuDwPs_X_ object and prepare it for insert into the target BioPs_Y_ table
		@return: the sa add object 
	"""
	pass
