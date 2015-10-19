from sqlalchemy import exists
from models.biopublicmodels import People

def processPerson(srcPerson, sesTarget ):
	"""Testing if i can functionalize my program
		
		Takes in a source person object from biopsmodels (mysql.bio_ps.people)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.people).
	"""

	def personExists(emplid):
		"""determine the person exists in the target database."""
		return sesTarget.query(
			exists().where(
				People.emplid == emplid ) )
	
	
	if personExists(srcPerson.emplid):

		def personUpdateRequired(emplid, source_hash):
			"""
				Determine if the person that exists requires an update.
				@True: returned from this function if hash is unchangeed
				@False: returned if hash is changed, indicating a need to updated record.
			"""
			return not sesTarget.query(
				exists().where(
					People.emplid == emplid ).where(
					People.source_hash == source_hash )	)
		
		if personUpdateRequired( srcPerson.emplid, srcPerson.source_hash ):
			# update the the database with the data changes.
			updatePerson = sesTarget.query( 
				People ).filter(
					People.emplid == srcPerson.emplid ).one()

			updatePerson.source_hash = srcPerson.source_hash
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
			raise TypeError('person exists BUT personupdaterequired is false!')
			
	else:
		# person wasn't in the target databases, add them now
		insertPerson = People(
			source_hash = srcPerson.source_hash,
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
