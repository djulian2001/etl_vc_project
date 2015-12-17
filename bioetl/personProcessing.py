import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import People
from models.asudwpsmodels import AsuDwPsPerson, AsuPsBioFilters

def getSourcePeople( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of person records from the people table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList(True)

	return sesSource.query( 
		AsuDwPsPerson ).join(
			srcEmplidsSubQry, AsuDwPsPerson.emplid==srcEmplidsSubQry.c.emplid).order_by(
				AsuDwPsPerson.emplid).all()
	
def processPerson( srcPerson, sesTarget ):
	"""
		Takes in a source person object from biopsmodels (mysql.bio_ps.people)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.people), or that nothing needs doing.

		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

	true, false = literal(True), literal(False)

	personList = [
		srcPerson.emplid,
		srcPerson.asurite_id,
		srcPerson.asu_id,
		srcPerson.ferpa,
		srcPerson.last_name,
		srcPerson.first_name,
		srcPerson.middle_name,
		srcPerson.display_name,
		srcPerson.preferred_first_name,
		srcPerson.affiliations,
		srcPerson.email_address,
		srcPerson.eid,
		srcPerson.birthdate,
		srcPerson.last_update ]

	srcHash = hashThisList( personList )

  	def personExists():
		"""
			Determine the person exists in the target database.
			@True: The person exists in the database
			@False: The person does not exist in the database

		"""
		(ret, ), = sesTarget.query(
			exists().where(
				People.emplid == srcPerson.emplid ) )
		
		return ret


	if personExists():
		def personUpdateRequired():
			"""
				Determine if the person that exists requires an update.
				@True: returned from this function if hash is unchangeed
				@False: returned if hash is changed, indicating a need to updated record.
			"""
			(ret, ), = sesTarget.query(
				exists().where(
					People.emplid == srcPerson.emplid ).where(
					People.source_hash == srcHash ).where(
					People.deleted_at.is_( None ) )	)

			return not ret
		
		if personUpdateRequired():
			# update the database with the source data and return the target object with changes.
			updatePerson = sesTarget.query( 
				People ).filter(
					People.emplid == srcPerson.emplid ).one()

			updatePerson.source_hash = srcHash
			updatePerson.emplid = srcPerson.emplid
			updatePerson.asurite_id = srcPerson.asurite_id
			updatePerson.asu_id = srcPerson.asu_id
			updatePerson.ferpa = srcPerson.ferpa
			updatePerson.last_name = srcPerson.last_name
			updatePerson.first_name = srcPerson.first_name
			updatePerson.middle_name = srcPerson.middle_name
			updatePerson.display_name = srcPerson.display_name
			updatePerson.preferred_first_name = srcPerson.preferred_first_name
			updatePerson.affiliations = srcPerson.affiliations
			updatePerson.email_address = srcPerson.email_address
			updatePerson.eid = srcPerson.eid
			updatePerson.birthdate = srcPerson.birthdate
			updatePerson.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updatePerson.deleted_at = None

			return updatePerson 
		else:
			raise TypeError('source person already exists and requires no updates!')

	else:
		# person wasn't in the target databases, add them now
		insertPerson = People(
			source_hash = srcHash,
			emplid = srcPerson.emplid,
			asurite_id = srcPerson.asurite_id,
			asu_id = srcPerson.asu_id,
			ferpa = srcPerson.ferpa,
			last_name = srcPerson.last_name,
			first_name = srcPerson.first_name,
			middle_name = srcPerson.middle_name,
			display_name = srcPerson.display_name,
			preferred_first_name = srcPerson.preferred_first_name,
			affiliations = srcPerson.affiliations,
			email_address = srcPerson.email_address,
			eid = srcPerson.eid,
			birthdate = srcPerson.birthdate,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )
		
		return insertPerson

def getTargetPeople( sesTarget ):
	"""
		Returns a set of People object from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
			People ).filter(
				People.deleted_at.is_( None ) ).all()


def softDeletePerson( tgtRecord, srcRecords ):
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
		return not any( srcRecord.emplid == tgtRecord.emplid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')




