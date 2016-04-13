import datetime, re

from bioetl.sharedProcesses import hashThisList
from models.biopublicmodels import People, Phones
from models.asudwpsmodels import AsuDwPsPhones, AsuPsBioFilters

def getTableName():
	return Phones.__table__.name

def getSourceData( sesSource, qryList=None ):
	"""Returns a collection of phone records from the source database"""
	if not qryList:
		srcFilters = AsuPsBioFilters( sesSource )

		srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

		return sesSource.query( 
			AsuDwPsPhones ).join(
				srcEmplidsSubQry, AsuDwPsPhones.emplid==srcEmplidsSubQry.c.emplid ).filter(
					AsuDwPsPhones.phone_type.in_( ('WORK','CELL') ) ).all()
	else:
		return sesSource.query(
			AsuDwPsPhones ).filter(
				AsuDwPsPhones.emplid.in_( qryList ) ).filter(
				AsuDwPsPhones.phone_type.in_( ('WORK','CELL') ) ).all()

def cleanPhoneNumber( phone ):
	""""this will clean up the phone numbers."""
	return re.sub( "[^0-9\+]", "", phone )


def processData( srcPersonPhone, sesTarget ):
	"""
		Takes in a source AsuDwPsPhone object from the asudw database
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.person_phones), or that nothing needs doing, but each
		source record will have an action in the target database via the
		updated_flag.
	"""	

	def getTargetRecords():
		"""Returns a record set from the target database."""
		ret = sesTarget.query(
			Phones ).filter( 
				Phones.emplid == srcPersonPhone.emplid ).filter(
				Phones.phone_type == srcPersonPhone.phone_type ).filter(
				Phones.updated_flag == False ).all()

		return ret

	recordToList = [
		srcPersonPhone.emplid,
		srcPersonPhone.phone_type,
		srcPersonPhone.phone,
		srcPersonPhone.last_update ]
	
	srcHash = hashThisList( recordToList )

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

			tgtRecord.source_hash = srcHash
			tgtRecord.updated_flag = True
			tgtRecord.phone = cleanPhoneNumber( srcPersonPhone.phone )
			tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			tgtRecord.deleted_at = None

			return tgtRecord

	else:
		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcPersonPhone.emplid ).one()

		insertPhone = Phones(
			person_id = srcGetPersonId.id,
			updated_flag = True,
			source_hash = srcHash,
			emplid = srcPersonPhone.emplid,
			phone_type = srcPersonPhone.phone_type,
			phone = cleanPhoneNumber( srcPersonPhone.phone ),
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertPhone


def getTargetData( sesTarget ):
	"""pass"""
	return sesTarget.query(
		Phones ).filter( 
			Phones.updated_flag == False ).filter(
			Phones.deleted_at.is_( None ) ).all()

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
			srcRecord.emplid == tgtRecord.emplid 
			and srcRecord.phone_type == tgtRecord.phone_type 
			and cleanPhoneNumber( srcRecord.phone ) == tgtRecord.phone for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
