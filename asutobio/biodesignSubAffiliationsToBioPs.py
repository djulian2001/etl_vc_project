from models.biopsmodels import BiodesignSubAffiliations

from sharedProcesses import hashThisList


# the data pull
def getSourceBiodesignSubAffiliationsData():
	"""
		The data for this table is provided by Biodesign directorate and will be 'seeded' from a data source.
		Within 
		@returns: a dictionary of seed records
	"""

	return BiodesignSubAffiliations.seedMe()

# the data load
def processBiodesignSubAffiliationData( aDict ):
	"""
		Process an BiodesignSubAffiliations object and prepare it for insert into the target BiodesignSubAffiliation table
		@return: the sa add object 
	"""
	
	biodesignSubAffiliationList = [
		aDict["code"],
		aDict["title"],
		aDict["description"],
		aDict["proximity_scope"],
		aDict["service_access"],
		aDict["distribution_lists"] ]

	biodesignSubAffiliationHash = hashThisList( biodesignSubAffiliationList )

	tgtbiodesignSubAffiliation = BiodesignSubAffiliations(
		source_hash = biodesignSubAffiliationHash,
		code = aDict["code"],
		title = aDict["title"],
		description = aDict["description"],
		proximity_scope = aDict["proximity_scope"],
		service_access = aDict["service_access"],
		distribution_lists = aDict["distribution_lists"] )

	return tgtbiodesignSubAffiliation