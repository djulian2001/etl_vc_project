import datetime, re
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import People, Phones
from models.asudwpsmodels import AsuDwPsPhones, AsuPsBioFilters


def getSourcePhones( sesSource ):
	"""Returns a collection of phone records from the source database"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

	return sesSource.query( 
		AsuDwPsPhones ).join(
			srcEmplidsSubQry, AsuDwPsPhones.emplid==srcEmplidsSubQry.c.emplid).order_by(
				AsuDwPsPhones.emplid).all()


def processPhone( srcPersonPhone, sesTarget ):
	"""
		Takes in source phone object and determines what shall be done with the object.
		Returns a Phone object to be either ignored, updated, or inserted into the target database
		
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)
	
	recordToList = [
		personPhone.emplid,
		personPhone.phone_type,
		personPhone.phone,
		personPhone.last_update ]
	
	srcHash = hashThisList( recordToList )

	def personPhoneExists():
		"""
			determine the person exists in the target database.
			@True: There is a phone record for emplid and type already in the target database
			@False: The record does not exist and requires insert.
		"""
		(ret, ), = sesTarget.query(
			exists().where( 
				Phones.emplid == srcPersonPhone.emplid ).where(
				Phones.phone_type == srcPersonPhone.phone_type ) )
		
		return ret

	if personPhoneExists():
		
		def cleanPhoneNumber():
			""""this will clean up the phone numbers."""
			return re.sub( "[^0-9\+]", "", srcPersonPhone.phone )

		def phoneRequiresUpdate():
			"""
				determine if the person that exists requires an update.
				@True:  The source record object requires update.
				@False:  The record doesn't require an update
			"""
			(ret, ), = sesTarget.query(
				exists().where(
					Phones.emplid == srcPersonPhone.emplid ).where(
					Phones.phone_type == srcPersonPhone.phone_type ).where(
					Phones.source_hash == srcHash ).where(	
					Phones.deleted_at.is_( None ) )	)

			return not ret

		if phoneRequiresUpdate():

			updatePersonPhone = sesTarget.query(
				Phones ).filter( 
					Phones.emplid == srcPersonPhone.emplid ).filter(
					Phones.phone_type == srcPersonPhone.phone_type ).filter(
					Phones.source_hash != srcHash ).filter(
					Phones.updated_flag == False ).first()

			updatePersonPhone.source_hash = srcHash
			updatePersonPhone.updated_flag = True
			updatePersonPhone.phone = cleanPhoneNumber()
			updatePersonPhone.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updatePersonPhone.deleted_at = None

			return updatePersonPhone

		else:
			raise TypeError('source person phone record already exists and requires no updates!')

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
			phone = cleanPhoneNumber(),
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertPhone


def getTargetPhones( sesTarget ):
	"""pass"""
	return sesTarget.query(
		Phones ).filter( 
			Phones.updated_flag == False ).filter(
			Phones.deleted_at.is_( None ) ).all()


		# .join(
		# People ).filter(
		# 	People.deleted_at.isnot( None ) ).all()

def cleanupSourcePhones( tgtRecord, srcRecords ):
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
			and srcRecord.phone == tgtRecord.phone for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')


