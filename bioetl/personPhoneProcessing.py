import datetime, re
from sqlalchemy import exists, literal

from models.biopublicmodels import People, Phones
from asutobio.models.biopsmodels import BioPsPhones


def getSourcePhones( sesSource ):
	"""Returns a collection of phone records from the source database"""

	return sesSource.query(
		BioPsPhones ).group_by(
			BioPsPhones.emplid ).group_by(
			BioPsPhones.phone_type ).group_by(
			BioPsPhones.source_hash ).all()

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
					Phones.source_hash == srcPersonPhone.source_hash )	)

			return not ret

		if phoneRequiresUpdate():

			updatePersonPhone = sesTarget.query(
				Phones ).filter( 
					Phones.emplid == srcPersonPhone.emplid ).filter(
					Phones.phone_type == srcPersonPhone.phone_type ).filter(
					Phones.source_hash != srcPersonPhone.source_hash ).filter(
					Phones.updated_flag == False ).first()

			updatePersonPhone.source_hash = srcPersonPhone.source_hash
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
			source_hash = srcPersonPhone.source_hash,
			emplid = srcPersonPhone.emplid,
			phone_type = srcPersonPhone.phone_type,
			phone = cleanPhoneNumber(),
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertPhone


def getTargetPhones( sesTarget ):
	"""pass"""
	return sesTarget.query(
		Phones ).filter( 
			Phones.updated_flag == False ).join(
		People ).filter(
			People.deleted_at.isnot( None ) ).all()

def cleanupSourcePhones( tgtMissingPersonPhone, sesSource ):
	"""Determine if an existing phone for an active person is still an active phone for that person."""
	
	def removeExistingPhone( emplid, phone_type, phone ):
		"""
			Does the phone in the target database exist in the source database:
			@True: The phone was not found and should be removed.
			@False: The phone was found and should not be removed.
		"""
		(ret, ), = sesSource.query(
			exists().where( 
				BioPsPhones.emplid == emplid ).where(
				BioPsPhones.phone_type == phone_type ).where(
				BioPsPhones.phone == phone) )

		return ret

	if removeExistingPhone( tgtMissingPersonPhone.emplid, tgtMissingPersonPhone.phone_type, tgtMissingPersonPhone.phone ):

		tgtMissingPersonPhone.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		
		return tgtMissingPersonPhone
	else:
		raise TypeError('source person phone is active and requires no removal.')

