import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import PersonWebProfile, People

def processPersonWebProfile( srcPersonWebProfile, sesTarget ):
	"""
		Takes in a source PersonWebProfile object from biopsmodels (mysql.bio_ps.PersonWebProfile)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonWebProfile), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

	true, false = literal(True), literal(False)

	personWebProfileList = [
		srcPersonWebProfile.emplid,
		srcPersonWebProfile.bio,
		srcPersonWebProfile.research_interests,
		srcPersonWebProfile.cv,
		srcPersonWebProfile.website,
		srcPersonWebProfile.teaching_website,
		srcPersonWebProfile.grad_faculties,
		srcPersonWebProfile.professional_associations,
		srcPersonWebProfile.work_history,
		srcPersonWebProfile.education,
		srcPersonWebProfile.research_group,
		srcPersonWebProfile.research_website,
		srcPersonWebProfile.honors_awards,
		srcPersonWebProfile.editorships,
		srcPersonWebProfile.presentations ]

	if any( personWebProfileList[1:] ):
		srcHash = hashThisList( personWebProfileList )

		def personWebProfileExists():
			"""
				determine the personWebProfile exists in the target database.
				@True: The personWebProfile exists in the database
				@False: The personWebProfile does not exist in the database
			"""
			(ret, ), = sesTarget.query(
				exists().where(
					PersonWebProfile.emplid == srcPersonWebProfile.emplid ) )

			return ret

		if personWebProfileExists():

			def personWebProfileUpdateRequired():
				"""
					Determine if the personWebProfile that exists requires and update.
					@True: returned if source_hash is unchanged
					@False: returned if source_hash is different
				"""	
				(ret, ), = sesTarget.query(
					exists().where(
						PersonWebProfile.emplid == srcPersonWebProfile.emplid ).where(
						PersonWebProfile.source_hash == srcHash ).where(	
						PersonWebProfile.deleted_at.is_( None ) ) )

				return not ret


			if personWebProfileUpdateRequired():
				# retrive the tables object to update.
				updatePersonWebProfile = sesTarget.query(
					PersonWebProfile ).filter(
						PersonWebProfile.emplid == srcPersonWebProfile.emplid ).one()

				# repeat the following pattern for all mapped attributes:
				updatePersonWebProfile.source_hash = srcHash
				updatePersonWebProfile.emplid = srcPersonWebProfile.emplid
				updatePersonWebProfile.bio
				updatePersonWebProfile.research_interests
				updatePersonWebProfile.cv
				updatePersonWebProfile.website
				updatePersonWebProfile.teaching_website
				updatePersonWebProfile.grad_faculties
				updatePersonWebProfile.professional_associations
				updatePersonWebProfile.work_history
				updatePersonWebProfile.education
				updatePersonWebProfile.research_group
				updatePersonWebProfile.research_website
				updatePersonWebProfile.honors_awards
				updatePersonWebProfile.editorships
				updatePersonWebProfile.presentations
				updatePersonWebProfile.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
				updatePersonWebProfile.deleted_at = None

				return updatePersonWebProfile
			else:
				raise TypeError('source personWebProfile already exists and requires no updates!')
				
		else:
			
			srcGetPersonId = sesTarget.query(
				People.id ).filter(
					People.emplid == srcPersonWebProfile.emplid ).one()

			insertPersonWebProfile = PersonWebProfile(
				person_id = srcGetPersonId.id,
				source_hash = srcHash,
				emplid = srcPersonWebProfile.emplid,
				bio = srcPersonWebProfile.bio,
				research_interests = srcPersonWebProfile.research_interests,
				cv = srcPersonWebProfile.cv,
				website = srcPersonWebProfile.website,
				teaching_website = srcPersonWebProfile.teaching_website,
				grad_faculties = srcPersonWebProfile.grad_faculties,
				professional_associations = srcPersonWebProfile.professional_associations,
				work_history = srcPersonWebProfile.work_history,
				education = srcPersonWebProfile.education,
				research_group = srcPersonWebProfile.research_group,
				research_website = srcPersonWebProfile.research_website,
				honors_awards = srcPersonWebProfile.honors_awards,
				editorships = srcPersonWebProfile.editorships,
				presentations = srcPersonWebProfile.presentations,
				created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

			return insertPersonWebProfile
	else:
		return None

def getTargetPersonWebProfiles( sesTarget ):
	"""
		Returns a set of PersonWebProfile objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		PersonWebProfile ).filter(
			PersonWebProfile.deleted_at.is_( None ) ).all()

def softDeletePersonWebProfile( tgtRecord, srcRecords ):
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

