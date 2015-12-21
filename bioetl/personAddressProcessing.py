import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import People, Addresses
from models.asudwpsmodels import AsuDwPsAddresses, AsuPsBioFilters

def getSourceAddresses( sesSource ):
	"""Get and return the source database records for the persons address data."""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

	return sesSource.query(
		AsuDwPsAddresses ).join(
			srcEmplidsSubQry, AsuDwPsAddresses.emplid==srcEmplidsSubQry.c.emplid ).filter(
				AsuDwPsAddresses.address_type=='CLOC' ).order_by(
					AsuDwPsAddresses.emplid ).all()

def processAddress( srcPersonAddress, sesTarget ):
	"""
		Determine what processing action is required for a person address record selected
		from the source database.  Using the target database as the test, a record will be
		either returned as ingored record (no changes), insert new object, or update an
		existing object.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

	true, false = literal(True), literal(False)

	recordToList = [
		srcPersonAddress.emplid,
		srcPersonAddress.address_type,
		srcPersonAddress.address1,
		srcPersonAddress.address2,
		srcPersonAddress.address3,
		srcPersonAddress.address4,
		srcPersonAddress.city,
		srcPersonAddress.state,
		srcPersonAddress.postal,
		srcPersonAddress.country_code,
		srcPersonAddress.country_descr,
		srcPersonAddress.last_update ]

	srcHash = hashThisList( recordToList )

	def personAddressExists():
		"""
			Determine the person address exists in the target database.
			@True: The person address exists and requires update checks
			@False: The person doesn't exist and will prepare for insert
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				Addresses.emplid == srcPersonAddress.emplid ).where(
				Addresses.address_type == srcPersonAddress.address_type ).where(
				Addresses.updated_flag == False ) )

		return ret

	if personAddressExists():

		def addressRequiresUpdate():
			"""
				Determine that the address record from the source database requires update
				@True: The record was not found and in the target database and should be updated
				@False: The record was found and should be ingnored with no changes required
			"""
			(ret, ), = sesTarget.query(
				exists().where( 
					Addresses.emplid == srcPersonAddress.emplid ).where( 
					Addresses.address_type == srcPersonAddress.address_type ).where(
					Addresses.source_hash == srcHash ).where(
					Addresses.deleted_at.is_( None ) ).where(
					Addresses.updated_flag == False ) )

			return not ret

		if addressRequiresUpdate():
			
			updatePersonAddress = sesTarget.query(
				Addresses ).filter( 
					Addresses.emplid == srcPersonAddress.emplid ).filter( 
					Addresses.address_type == srcPersonAddress.address_type ).filter( 
					Addresses.source_hash != srcHash ).filter(
					Addresses.updated_flag == False ).first()

			# the record in the target database that will be updated.
			updatePersonAddress.source_hash = srcHash
			updatePersonAddress.updated_flag = True
			updatePersonAddress.address1 = srcPersonAddress.address1
			updatePersonAddress.address2 = srcPersonAddress.address2
			updatePersonAddress.address3 = srcPersonAddress.address3
			updatePersonAddress.address4 = srcPersonAddress.address4
			updatePersonAddress.city = srcPersonAddress.city
			updatePersonAddress.state = srcPersonAddress.state
			updatePersonAddress.postal = srcPersonAddress.postal
			updatePersonAddress.country_code = srcPersonAddress.country_code
			updatePersonAddress.country_descr = srcPersonAddress.country_descr
			# update the bio timestamp
			updatePersonAddress.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			updatePersonAddress.deleted_at = None
			# add the object to the session to commit the updated
			return updatePersonAddress

		else:
			raise TypeError('Person address record required no insert or updates.')
	else:
		getPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcPersonAddress.emplid ).one()

		insertPersonAddress = Addresses(
			person_id = getPersonId.id,
			source_hash = srcHash,
			updated_flag = True,
			emplid = srcPersonAddress.emplid,
			address_type = srcPersonAddress.address_type,
			address1 = srcPersonAddress.address1,
			address2 = srcPersonAddress.address2,
			address3 = srcPersonAddress.address3,
			address4 = srcPersonAddress.address4,
			city = srcPersonAddress.city,
			state = srcPersonAddress.state,
			postal = srcPersonAddress.postal,
			country_code = srcPersonAddress.country_code,
			country_descr = srcPersonAddress.country_descr,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertPersonAddress

def getTargetAddresses( sesTarget ):
	"""Get a set of person addresses from the target database"""
	return sesTarget.query(
		Addresses ).filter( 
			Addresses.updated_flag == False ).filter(
			Addresses.deleted_at.is_( None ) ).all()

def cleanupSourceAddresses( tgtRecord, srcRecords ):
	"""
		The list of source records changes as time moves on, the source records
		removed from the list are not deleted, but flaged removed by the 
		deleted_at field.

		The return of this function returns a sqlalchemy object to update a target record object.
	"""
	def dataMissing():
		"""
			The origional list of selected data is then used to see if data requires a soft-delete
			@True: Means update the records deleted_at column
			@False: Do nothing
		"""
		return not any(
			srcRecord.emplid == tgtRecord.emplid and
			srcRecord.address_type == tgtRecord.address_type and
			srcRecord.address1 == tgtRecord.address1 and
			srcRecord.address2 == tgtRecord.address2 and
			srcRecord.address3 == tgtRecord.address3 and
			srcRecord.address4 == tgtRecord.address4 for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')



