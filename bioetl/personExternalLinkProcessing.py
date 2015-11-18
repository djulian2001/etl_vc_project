import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import PersonExternalLinks, People
from asutobio.models.biopsmodels import BioPsPersonExternalLinks

#template mapping... plural PersonExternalLinks    singularCaped PersonExternalLink   singularLower personExternalLink 

def getSourcePersonExternalLinks( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the PersonExternalLinks table of the source database.
	"""

	return sesSource.query( BioPsPersonExternalLinks ).all()

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

#template mapping... column where(s) emplid 
	true, false = literal(True), literal(False)

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
					PersonExternalLinks.source_hash == srcPersonExternalLink.source_hash ) )

			return not ret

		if personExternalLinkUpdateRequired():
			# retrive the tables object to update.
			updatePersonExternalLink = sesTarget.query(
				PersonExternalLinks ).filter(
					PersonExternalLinks.emplid == srcPersonExternalLink.emplid ).one()

			# repeat the following pattern for all mapped attributes:
			updatePersonExternalLink.source_hash = srcPersonExternalLink.source_hash
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
			source_hash = srcPersonExternalLink.source_hash,
			person_id = srcGetPersonId.id,
			emplid = srcPersonExternalLink.emplid,
			facebook = srcPersonExternalLink.facebook,
			twitter = srcPersonExternalLink.twitter,
			google_plus = srcPersonExternalLink.google_plus,
			linkedin = srcPersonExternalLink.linkedin,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertPersonExternalLink

def getTargetPersonExternalLinks( sesTarget ):
	"""
		Returns a set of PersonExternalLinks objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		PersonExternalLinks ).filter(
			PersonExternalLinks.deleted_at.is_( None ) ).all()

def softDeletePersonExternalLink( tgtMissingPersonExternalLink, sesSource ):
	"""
		The list of PersonExternalLinks changes as time moves on, the PersonExternalLinks removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagPersonExternalLinkMissing():
		"""
			Determine that the personExternalLink object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsPersonExternalLinks.emplid == tgtMissingPersonExternalLink.emplid ) )

		return not ret

	if flagPersonExternalLinkMissing():

		tgtMissingPersonExternalLink.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingPersonExternalLink

	else:
		raise TypeError('source person still exists and requires no soft delete!')

