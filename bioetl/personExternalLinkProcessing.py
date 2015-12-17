import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import PersonExternalLinks, People

#template mapping... plural PersonExternalLinks    singularCaped PersonExternalLink   singularLower personExternalLink 

# change value to the singular
def processPersonExternalLink( srcPersonExternalLink, sesTarget ):
	"""
		Takes in a source PersonExternalLink object from biopsmodels (mysql.bio_ps.PersonExternalLinks)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.PersonExternalLinks), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

	true, false = literal(True), literal(False)

	recordToList = [
		srcPerson.emplid,
		srcPerson.facebook,
		srcPerson.twitter,
		srcPerson.google_plus,
		srcPerson.linkedin ]

	if any( recordToList[1:] ):
		srcHash = hashThisList( srcSubAffiliation.values() )

		def personExternalLinkExists():
			"""
				determine the personExternalLink exists in the target database.
				@True: The personExternalLink exists in the database
				@False: The personExternalLink does not exist in the database
			"""
			(ret, ), = sesTarget.query(
				exists().where(
					PersonExternalLinks.emplid == srcPersonExternalLink.emplid ) )

			return ret

		if personExternalLinkExists():

			def personExternalLinkUpdateRequired():
				"""
					Determine if the personExternalLink that exists requires and update.
					@True: returned if source_hash is unchanged
					@False: returned if source_hash is different
				"""	
				(ret, ), = sesTarget.query(
					exists().where(
						PersonExternalLinks.emplid == srcPersonExternalLink.emplid ).where(
						PersonExternalLinks.source_hash == srcHash ).where(	
						PersonExternalLinks.deleted_at.is_( None ) ) )

				return not ret


			if personExternalLinkUpdateRequired():
				# retrive the tables object to update.
				updatePersonExternalLink = sesTarget.query(
					PersonExternalLinks ).filter(
						PersonExternalLinks.emplid == srcPersonExternalLink.emplid ).one()

				# repeat the following pattern for all mapped attributes:
				updatePersonExternalLink.source_hash = srcHash
				updatePersonExternalLink.emplid = srcPersonExternalLink.emplid
				updatePersonExternalLink.facebook = srcPersonExternalLink.facebook
				updatePersonExternalLink.twitter = srcPersonExternalLink.twitter
				updatePersonExternalLink.google_plus = srcPersonExternalLink.google_plus
				updatePersonExternalLink.linkedin = srcPersonExternalLink.linkedin
				updatePersonExternalLink.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
				updatePersonExternalLink.deleted_at = None

				return updatePersonExternalLink
			else:
				raise TypeError('source personExternalLink already exists and requires no updates!')

		else:
			srcGetPersonId = sesTarget.query(
				People.id ).filter(
					People.emplid == srcPersonExternalLink.emplid ).one()

			insertPersonExternalLink = PersonExternalLinks(
				source_hash = srcHash,
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


def getTargetPersonExternalLinks( sesTarget ):
	"""
		Returns a set of PersonExternalLinks objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		PersonExternalLinks ).filter(
			PersonExternalLinks.deleted_at.is_( None ) ).all()


def softDeletePersonExternalLink( tgtRecord, srcRecords ):
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


