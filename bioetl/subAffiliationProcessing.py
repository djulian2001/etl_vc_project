import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import SubAffiliations
from asutobio.models.biopsmodels import BiodesignSubAffiliations

#template mapping... plural SubAffiliations    singularCaped SubAffiliation   singularLower subAffiliation 

def getSourceSubAffiliations( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the SubAffiliations table of the source database.
	"""

	return sesSource.query( BiodesignSubAffiliations ).all()

# change value to the singular
def processSubAffiliation( srcSubAffiliation, sesTarget ):
	"""
		Takes in a source SubAffiliation object from biopsmodels (mysql.bio_ps.SubAffiliations)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.SubAffiliations), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

	def subAffiliationExists():
		"""
			determine the subAffiliation exists in the target database.
			@True: The subAffiliation exists in the database
			@False: The subAffiliation does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				SubAffiliations.code == srcSubAffiliation.code ) )

		return ret

	if subAffiliationExists():

		def subAffiliationUpdateRequired():
			"""
				Determine if the subAffiliation that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					SubAffiliations.code == srcSubAffiliation.code ).where(
					SubAffiliations.source_hash == srcSubAffiliation.source_hash ) )

			return not ret

		if subAffiliationUpdateRequired():
			# retrive the tables object to update.
			updateSubAffiliation = sesTarget.query(
				SubAffiliations ).filter(
					SubAffiliations.code == srcSubAffiliation.code ).one()

			# repeat the following pattern for all mapped attributes:
			updateSubAffiliation.source_hash = srcSubAffiliation.source_hash
			updateSubAffiliation.code = srcSubAffiliation.code
			updateSubAffiliation.title = srcSubAffiliation.title
			updateSubAffiliation.description = srcSubAffiliation.description
			updateSubAffiliation.proximity_scope = srcSubAffiliation.proximity_scope
			updateSubAffiliation.service_access = srcSubAffiliation.service_access
			updateSubAffiliation.distribution_lists = srcSubAffiliation.distribution_lists
			updateSubAffiliation.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateSubAffiliation.deleted_at = None

			return updateSubAffiliation
		else:
			raise TypeError('source subAffiliation already exists and requires no updates!')

	else:
		insertSubAffiliation = SubAffiliations(
			source_hash = srcSubAffiliation.source_hash,
			code = srcSubAffiliation.code,
			title = srcSubAffiliation.title,
			description = srcSubAffiliation.description,
			proximity_scope = srcSubAffiliation.proximity_scope,
			service_access = srcSubAffiliation.service_access,
			distribution_lists = srcSubAffiliation.distribution_lists,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertSubAffiliation

def getTargetSubAffiliations( sesTarget ):
	"""
		Returns a set of SubAffiliations objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		SubAffiliations ).filter(
			SubAffiliations.deleted_at.is_( None ) ).all()

def softDeleteSubAffiliation( tgtMissingSubAffiliation, sesSource ):
	"""
		The list of SubAffiliations changes as time moves on, the SubAffiliations removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a subAffiliation object.
	"""

	def flagSubAffiliationMissing():
		"""
			Determine that the subAffiliation object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BiodesignSubAffiliations.code == tgtMissingSubAffiliation.code ) )

		return not ret

	if flagSubAffiliationMissing():

		tgtMissingSubAffiliation.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingSubAffiliation

	else:
		raise TypeError('source subAffiliation still exists and requires no soft delete!')


