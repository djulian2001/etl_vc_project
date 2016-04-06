import datetime

from sharedProcesses import hashThisList
from models.biopublicmodels import PersonWebProfile, People

def getTableName():
	return PersonWebProfile.__table__.name

def processData( srcPersonWebProfile, sesTarget ):
	"""
		Takes in a source PersonWebProfile object from the asudw as a person record 
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonWebProfile), or that nothing needs doing, but each
		source record will have an action in the target database via the
		updated_flag.
	"""
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


		def getTargetRecords():
			"""Returns a record set from the target database."""

			ret = sesTarget.query(
				PersonWebProfile ).filter(
					PersonWebProfile.emplid == srcPersonWebProfile.emplid ).filter(
					PersonWebProfile.updated_flag == False ).all()

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

				# repeat the following pattern for all mapped attributes:
				tgtRecord.source_hash = srcHash
				tgtRecord.updated_flag = True
				tgtRecord.emplid = srcPersonWebProfile.emplid
				tgtRecord.bio = srcPersonWebProfile.bio
				tgtRecord.research_interests = srcPersonWebProfile.research_interests
				tgtRecord.cv = srcPersonWebProfile.cv
				tgtRecord.website = srcPersonWebProfile.website
				tgtRecord.teaching_website = srcPersonWebProfile.teaching_website
				tgtRecord.grad_faculties = srcPersonWebProfile.grad_faculties
				tgtRecord.professional_associations = srcPersonWebProfile.professional_associations
				tgtRecord.work_history = srcPersonWebProfile.work_history
				tgtRecord.education = srcPersonWebProfile.education
				tgtRecord.research_group = srcPersonWebProfile.research_group
				tgtRecord.research_website = srcPersonWebProfile.research_website
				tgtRecord.honors_awards = srcPersonWebProfile.honors_awards
				tgtRecord.editorships = srcPersonWebProfile.editorships
				tgtRecord.presentations = srcPersonWebProfile.presentations
				tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
				tgtRecord.deleted_at = None

				return tgtRecord
				
		else:
			
			srcGetPersonId = sesTarget.query(
				People.id ).filter(
					People.emplid == srcPersonWebProfile.emplid ).one()

			insertPersonWebProfile = PersonWebProfile(
				person_id = srcGetPersonId.id,
				# person_id = person.id,
				source_hash = srcHash,
				updated_flag = True,
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

def getTargetData( sesTarget ):
	"""
		Returns a set of PersonWebProfile objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		PersonWebProfile ).filter(
			PersonWebProfile.deleted_at.is_( None ) ).all()

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
		return not any( srcRecord.emplid == tgtRecord.emplid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	# else:
	# 	raise TypeError('source target record still exists and requires no soft delete!')

