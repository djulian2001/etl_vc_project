from models.asudwpsmodels import AsuDwPsAddresses, AsuPsBioFilters
from models.biopsmodels import BioPsAddresses 

from sharedProcesses import hashThisList

# the data pull
def getSourceAddresses( sesSource ):
	"""
		Selects the data from the data wharehouse for the Addresses model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

	return sesSource.query(
		AsuDwPsAddresses ).join(
			srcEmplidsSubQry, AsuDwPsAddresses.emplid==srcEmplidsSubQry.c.emplid ).filter(
				AsuDwPsAddresses.address_type=='CLOC' ).order_by(
					AsuDwPsAddresses.emplid ).all()

# the data load
def processAddressesData( personAddress ):
	"""
		Process an AsuDwPsPerson object and prepare it for insert into the target BioPsPeople table
		@return: the sa add object 
	"""

	personAddressList = [
		personAddress.emplid,
		personAddress.address_type,
		personAddress.address1,
		personAddress.address2,
		personAddress.address3,
		personAddress.address4,
		personAddress.city,
		personAddress.state,
		personAddress.postal,
		personAddress.country_code,
		personAddress.country_descr,
		personAddress.last_update ]

	personAddressHash = hashThisList( personAddressList )

	tgtPersonAddress = BioPsAddresses(
		source_hash = personAddressHash,
		emplid = personAddress.emplid,
		address_type = personAddress.address_type,
		address1 = personAddress.address1,
		address2 = personAddress.address2,
		address3 = personAddress.address3,
		address4 = personAddress.address4,
		city = personAddress.city,
		state = personAddress.state,
		postal = personAddress.postal,
		country_code = personAddress.country_code,
		country_descr = personAddress.country_descr,
		last_update = personAddress.last_update)

	return tgtPersonAddress
