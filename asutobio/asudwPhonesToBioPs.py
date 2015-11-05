from models.asudwpsmodels import AsuDwPsPhones, AsuPsBioFilters
from models.biopsmodels import BioPsPhones

from sharedProcesses import hashThisList

# the data pull
def getSourcePhonesData( sesSource ):
	"""
		Selects the data from the data wharehouse for the Phones model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsPhones ).join(
			srcEmplidsSubQry, AsuDwPsPhones.emplid==srcEmplidsSubQry.c.emplid ).filter(
				AsuDwPsPhones.phone_type.in_( 
					['CELL','WORK'] ) ).order_by(
						AsuDwPsPhones.emplid ).all()

# the data load
def processPhonesData( personPhone ):
	"""
		Process an AsuDwPsPhones object and prepare it for insert into the target BioPsPhones table
		@return: the sa add object
	"""

	personPhoneList = [
		personPhone.emplid,
		personPhone.phone_type,
		personPhone.phone,
		personPhone.last_update ]

	personPhoneHash = hashThisList( personPhoneList )

	tgtPersonPhone = BioPsPhones(
		source_hash = personPhoneHash,
		emplid = personPhone.emplid,
		phone_type = personPhone.phone_type,
		phone = personPhone.phone,
		last_update = personPhone.last_update )

	return tgtPersonPhone