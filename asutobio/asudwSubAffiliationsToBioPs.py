from models.asudwpsmodels import AsuDwPsSubAffiliations, AsuPsBioFilters
from models.biopsmodels import BioPsSubAffiliations

from sharedProcesses import hashThisList


# the data pull
def getSourceSubAffiliationsData( sesSource ):
	"""
		Selects the data from the data wharehouse for the SubAffiliations model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsSubAffiliations ).join(
			srcEmplidsSubQry, AsuDwPsSubAffiliations.emplid==srcEmplidsSubQry.c.emplid ).order_by(
				AsuDwPsSubAffiliations.emplid ).all()

# the data load
def processSubAffiliationsData( personSubAffiliation ):
	"""
		Process an AsuDwPsSubAffiliations object and prepare it for insert into the target BioPsSubAffiliations table
		@return: the sa add object 
	"""
	personSubAffiliationList = [
		personSubAffiliation.emplid,
		personSubAffiliation.deptid,
		personSubAffiliation.subaffiliation_code,
		personSubAffiliation.campus,
		personSubAffiliation.title,
		personSubAffiliation.short_description,
		personSubAffiliation.description,
		personSubAffiliation.directory_publish,
		personSubAffiliation.department,
		personSubAffiliation.last_update,
		personSubAffiliation.department_directory ]

	personSubAffiliationHash = hashThisList( personSubAffiliationList )

	tgtPersonSubAffiliation = BioPsSubAffiliations(
		source_hash = personSubAffiliationHash,
		emplid = personSubAffiliation.emplid,
		deptid = personSubAffiliation.deptid,
		subaffiliation_code = personSubAffiliation.subaffiliation_code,
		campus = personSubAffiliation.campus,
		title = personSubAffiliation.title,
		short_description = personSubAffiliation.short_description,
		description = personSubAffiliation.description,
		directory_publish = personSubAffiliation.directory_publish,
		department = personSubAffiliation.department,
		last_update = personSubAffiliation.last_update,
		department_directory = personSubAffiliation.department_directory )

	return tgtPersonSubAffiliation