import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import People, Addresses
from asutobio.models.biopsmodels import BioPsAddresses

def getSourceAddresses( sesSource ):
	"""Get and return the source database records for the persons address data."""
	return sesSource.query(
		BioPsAddresses ).group_by(
			BioPsAddresses.emplid ).group_by(
			BioPsAddresses.address_type ).group_by(
			BioPsAddresses.source_hash ).all()


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

	def personAddressExists( emplid, address_type ):
		"""
			Determine the person address exists in the target database.
			@True: The person address exists and requires update checks
			@False: The person doesn't exist and will prepare for insert
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				Addresses.emplid == emplid ).where(
				Addresses.address_type == address_type ) )

		return ret

	if personAddressExists( srcPersonAddress.emplid, srcPersonAddress.address_type ):

		def addressRequiresUpdate( emplid, address_type, source_hash ):
			"""
				Determine that the address record from the source database requires update
				@True: The record was not found and in the target database and should be updated
				@False: The record was found and should be ingnored with no changes required
			"""
			(ret, ), = sesTarget.query(
				exists().where( 
					Addresses.emplid == srcPersonAddress.emplid ).where( 
					Addresses.address_type == srcPersonAddress.address_type ).where(
					Addresses.source_hash == srcPersonAddress.source_hash ) )

			return not ret

		if addressRequiresUpdate( srcPersonAddress.emplid, srcPersonAddress.address_type, srcPersonAddress.source_hash ):
			
			updatePersonAddress = sesTarget.query(
				Addresses ).filter( 
					Addresses.emplid == srcPersonAddress.emplid ).filter( 
					Addresses.address_type == srcPersonAddress.address_type ).filter( 
					Addresses.source_hash != srcPersonAddress.source_hash ).filter(
					Addresses.updated_flag == False ).first()

			# the record in the target database that will be updated.
			updatePersonAddress.source_hash = srcPersonAddress.source_hash
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
			source_hash = srcPersonAddress.source_hash,
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
			Addresses.updated_flag == False ).join(
		People ).filter(
			People.deleted_at.isnot( None ) ).all()

def cleanupSourceAddresses( tgtPersonAddress, sesSource ):
	"""
		If the phone no longer is found we remove it but only if the person is active.
		Returns a record that is no longer found in the source database and needs to be
		removed from the target database.
	"""

	# def removeMissingAddress( emplid, address_type, address1, address2, address3, address4 ):
	def removeMissingAddress():
		"""
			Determine if the person address record from the source database exists.
			@True: If the person address record is not found
			@False: If the person address was found in the source database
		"""
		(ret, ), = sesSource.query(
			exists().where( 
				BioPsPhones.emplid == tgtPersonAddress.emplid ).where(
				BioPsPhones.address_type == tgtPersonAddress.address_type ).where(
				BioPsPhones.address1 == tgtPersonAddress.address1 ).where(
				BioPsPhones.address1 == tgtPersonAddress.address2 ).where(
				BioPsPhones.address1 == tgtPersonAddress.address3 ).where(
				BioPsPhones.address1 == tgtPersonAddress.address4 ) )

		return not ret

	if removeMissingAddress():
		
		tgtPersonAddress.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		
		return tgtPersonAddress
	else:
		raise TypeError('No need to remove the target database person address record.')



