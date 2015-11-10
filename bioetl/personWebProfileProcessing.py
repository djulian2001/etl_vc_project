import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import PersonWebProfile, People
from asutobio.models.biopsmodels import BioPsPersonWebProfile

#template mapping... plural PersonWebProfile    singularCaped PersonWebProfile   singularLower personWebProfile

def getSourcePersonWebProfile( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the PersonWebProfile table of the source database.
	"""

	return sesSource.query( BioPsPersonWebProfile ).all()

# change value to the singular
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

#template mapping... column where(s) emplid 
	true, false = literal(True), literal(False)

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
					PersonWebProfile.source_hash == srcPersonWebProfile.source_hash ) )

			return not ret

		if personWebProfileUpdateRequired():
			# retrive the tables object to update.
			updatePersonWebProfile = sesTarget.query(
				PersonWebProfile ).filter(
					PersonWebProfile.emplid == srcPersonWebProfile.emplid ).one()

			# repeat the following pattern for all mapped attributes:
			updatePersonWebProfile.source_hash = srcPersonWebProfile.source_hash
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
			source_hash = srcPersonWebProfile.source_hash,
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

def getTargetPersonWebProfiles( sesTarget ):
	"""
		Returns a set of PersonWebProfile objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		PersonWebProfile ).filter(
			PersonWebProfile.deleted_at.is_( None ) ).all()

def softDeletePersonWebProfile( tgtMissingPersonWebProfile, sesSource ):
	"""
		The list of PersonWebProfile changes as time moves on, the PersonWebProfile removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagPersonWebProfileMissing( emplid ):
		"""
			Determine that the personWebProfile object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsPersonWebProfile.emplid == emplid ) )

		return not ret

	if flagPersonWebProfileMissing( tgtMissingPersonWebProfile.emplid ):

		tgtMissingPersonWebProfile.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingPersonWebProfile

	else:
		raise TypeError('source person still exists and requires no soft delete!')
