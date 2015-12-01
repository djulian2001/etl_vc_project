import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarEditedbooks
from asutobio.models.biopsmodels import BioPsFarEditedbooks


def getSourceFarEditedbooks( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarEditedbooks table of the source database.
	"""

	return sesSource.query( BioPsFarEditedbooks ).all()

# change value to the singular
def processFarEditedbook( srcFarEditedbook, sesTarget ):
	"""
		Takes in a source FarEditedbook object from biopsmodels (mysql.bio_ps.FarEditedbooks)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarEditedbooks), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

	def farEditedbookExists():
		"""
			determine the farEditedbook exists in the target database.
			@True: The farEditedbook exists in the database
			@False: The farEditedbook does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarEditedbooks._yyy_ == srcFarEditedbook._yyy_ ) )

		return ret

	if farEditedbookExists():

		def farEditedbookUpdateRequired():
			"""
				Determine if the farEditedbook that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarEditedbooks._yyy_ == srcFarEditedbook._yyy_ ).where(
					FarEditedbooks.source_hash == srcFarEditedbook.source_hash ) )

			return not ret

		if farEditedbookUpdateRequired():
			# retrive the tables object to update.
			updateFarEditedbook = sesTarget.query(
				FarEditedbooks ).filter(
					FarEditedbooks._yyy_ == srcFarEditedbook._yyy_ ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarEditedbook.source_hash = srcFarEditedbook.source_hash
			updateFarEditedbook._yyy_ = srcFarEditedbook._yyy_

			updateFarEditedbook.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarEditedbook.deleted_at = None

			return updateFarEditedbook
		else:
			raise TypeError('source farEditedbook already exists and requires no updates!')

	else:
		insertFarEditedbook = FarEditedbooks(
			source_hash = srcFarEditedbook.source_hash,
			_yyy_ = srcFarEditedbook._yyy_,
			...
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarEditedbook

def getTargetFarEditedbooks( sesTarget ):
	"""
		Returns a set of FarEditedbooks objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		FarEditedbooks ).filter(
			FarEditedbooks.deleted_at.is_( None ) ).all()

def softDeleteFarEditedbook( tgtMissingFarEditedbook, sesSource ):
	"""
		The list of FarEditedbooks changes as time moves on, the FarEditedbooks removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarEditedbookMissing():
		"""
			Determine that the farEditedbook object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsFarEditedbooks._yyy_ == tgtMissingFarEditedbook._yyy_ ) )

		return not ret

	if flagFarEditedbookMissing():

		tgtMissingFarEditedbook.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarEditedbook

	else:
		raise TypeError('source person still exists and requires no soft delete!')

