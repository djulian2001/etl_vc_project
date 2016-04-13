import datetime

from bioetl.sharedProcesses import hashThisList
from models.biopublicmodels import PersonExternalLinks, People


def getTableName():
	return PersonExternalLinks.__table__.name

def processData( srcPersonExternalLink, sesTarget ):
	"""
		Takes in a source PersonExternalLink object from the asudw as a person record
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonExternalLinks), or that nothing needs doing, but each
		source record will have an action in the target database via the updated_flag.
	"""

	recordToList = [
		srcPersonExternalLink.emplid,
		srcPersonExternalLink.facebook,
		srcPersonExternalLink.twitter,
		srcPersonExternalLink.google_plus,
		srcPersonExternalLink.linkedin ]

	if any( recordToList[1:] ):
		srcHash = hashThisList( recordToList )

		def getTargetRecords():
			"""
				determine the personExternalLink exists in the target database.
				@True: The personExternalLink exists in the database
				@False: The personExternalLink does not exist in the database
			"""
			ret = sesTarget.query(
				PersonExternalLinks ).filter(
					PersonExternalLinks.emplid == srcPersonExternalLink.emplid ).filter(
					PersonExternalLinks.updated_flag == False ).all()

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
				tgtRecord.emplid = srcPersonExternalLink.emplid
				tgtRecord.facebook = srcPersonExternalLink.facebook
				tgtRecord.twitter = srcPersonExternalLink.twitter
				tgtRecord.google_plus = srcPersonExternalLink.google_plus
				tgtRecord.linkedin = srcPersonExternalLink.linkedin
				tgtRecord.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
				tgtRecord.deleted_at = None

				return tgtRecord

		else:
			srcGetPersonId = sesTarget.query(
				People.id ).filter(
					People.emplid == srcPersonExternalLink.emplid ).first()

			insertPersonExternalLink = PersonExternalLinks(
				source_hash = srcHash,
				updated_flag = True,
				person_id = srcGetPersonId.id,
				emplid = srcPersonExternalLink.emplid,
				facebook = srcPersonExternalLink.facebook,
				twitter = srcPersonExternalLink.twitter,
				google_plus = srcPersonExternalLink.google_plus,
				linkedin = srcPersonExternalLink.linkedin,
				created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

			return insertPersonExternalLink
	else:
		return None


def getTargetData( sesTarget ):
	"""
		Returns a set of PersonExternalLinks objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		PersonExternalLinks ).filter(
			PersonExternalLinks.deleted_at.is_( None ) ).all()


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


