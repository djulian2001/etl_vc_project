import datetime

from bioetl.sharedProcesses import hashThisList, AsuPsBioFilters
from models.biopublicmodels import People, Addresses
from models.asudwpsmodels import AsuDwPsAddresses

def getTableName():
	return Addresses.__table__.name


def getSourceData( sesSource, appState=None, qryList=None ):
	"""Get and return the source database records for the persons address data."""
	if not qryList:
		srcFilters = AsuPsBioFilters( sesSource, appState.subAffCodes )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

		return sesSource.query(
			AsuDwPsAddresses ).join(
				srcEmplidsSubQry, AsuDwPsAddresses.emplid==srcEmplidsSubQry.c.emplid ).filter(
					AsuDwPsAddresses.address_type=='CLOC' ).order_by(
						AsuDwPsAddresses.emplid ).all()
	else:
		return sesSource.query(
			AsuDwPsAddresses ).filter(
				AsuDwPsAddresses.emplid.in_( qryList ) ).all()

def processData( srcPersonAddress, sesTarget ):
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

	def getTargetRecords():
		"""
			Determine the person address exists in the target database.
			@True: The person address exists and requires update checks
			@False: The person doesn't exist and will prepare for insert
		"""
		ret = sesTarget.query(
			Addresses ).filter(
				Addresses.emplid == srcPersonAddress.emplid ).filter(
				Addresses.address_type == srcPersonAddress.address_type ).filter(
				Addresses.address_type == srcPersonAddress.address1 ).filter(
				Addresses.address_type == srcPersonAddress.city ).filter(
				Addresses.address_type == srcPersonAddress.postal ).filter(
				Addresses.updated_flag == False ).all()

		return ret

	tgtRecords = getTargetRecords()

	if tgtRecords:
		""" 
			If true then an update is required, else an insert is required
			@True:
				Because there might be many recornds returned from the db, a loop is required.
				Trying not to update the data if it is not required, but the source data will
				require an action.
				@Else Block (NO BREAK REACHED):
					If the condition is not reached in the for block the else block 
					will insure	that a record is updated.  
					It might not update the record that	was initially created previously,
					but all source data has to be represented in the target database.
			@False:
				insert the new data from the source database.
		"""
		for tgtRecord in tgtRecords:

			if tgtRecord.source_hash == srcHash:
				tgtRecord.updated_flag = True
				tgtRecord.deleted_at = None
				return tgtRecord
				break

		else: # NO BREAK REACHED
			tgtRecord = tgtRecords[0]
			# the record in the target database that will be updated.
			tgtRecord.source_hash = srcHash
			tgtRecord.updated_flag = True
			tgtRecord.address1 = srcPersonAddress.address1
			tgtRecord.address2 = srcPersonAddress.address2
			tgtRecord.address3 = srcPersonAddress.address3
			tgtRecord.address4 = srcPersonAddress.address4
			tgtRecord.city = srcPersonAddress.city
			tgtRecord.state = srcPersonAddress.state
			tgtRecord.postal = srcPersonAddress.postal
			tgtRecord.country_code = srcPersonAddress.country_code
			tgtRecord.country_descr = srcPersonAddress.country_descr
			# update the bio timestamp
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
			tgtRecord.deleted_at = None
			# add the object to the session to commit the updated
			return tgtRecord

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

def getTargetData( sesTarget ):
	"""Get a set of person addresses from the target database"""
	return sesTarget.query(
		Addresses ).filter( 
			Addresses.updated_flag == False ).filter(
			Addresses.deleted_at.is_( None ) ).all()

def softDeleteData( tgtRecord, srcRecords ):
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
	