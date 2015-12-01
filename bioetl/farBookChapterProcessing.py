import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarBookChapters
from asutobio.models.biopsmodels import BioPsFarBookChapters


def getSourceFarBookChapters( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarBookChapters table of the source database.
	"""

	return sesSource.query( BioPsFarBookChapters ).all()

# change value to the singular
def processFarBookChapter( srcFarBookChapter, sesTarget ):
	"""
		Takes in a source FarBookChapter object from biopsmodels (mysql.bio_ps.FarBookChapters)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarBookChapters), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

	def farBookChapterExists():
		"""
			determine the farBookChapter exists in the target database.
			@True: The farBookChapter exists in the database
			@False: The farBookChapter does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarBookChapters._yyy_ == srcFarBookChapter._yyy_ ) )

		return ret

	if farBookChapterExists():

		def farBookChapterUpdateRequired():
			"""
				Determine if the farBookChapter that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarBookChapters._yyy_ == srcFarBookChapter._yyy_ ).where(
					FarBookChapters.source_hash == srcFarBookChapter.source_hash ) )

			return not ret

		if farBookChapterUpdateRequired():
			# retrive the tables object to update.
			updateFarBookChapter = sesTarget.query(
				FarBookChapters ).filter(
					FarBookChapters._yyy_ == srcFarBookChapter._yyy_ ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarBookChapter.source_hash = srcFarBookChapter.source_hash
			updateFarBookChapter._yyy_ = srcFarBookChapter._yyy_

			updateFarBookChapter.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarBookChapter.deleted_at = None

			return updateFarBookChapter
		else:
			raise TypeError('source farBookChapter already exists and requires no updates!')

	else:
		insertFarBookChapter = FarBookChapters(
			source_hash = srcFarBookChapter.source_hash,
			_yyy_ = srcFarBookChapter._yyy_,
			...
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarBookChapter

def getTargetFarBookChapters( sesTarget ):
	"""
		Returns a set of FarBookChapters objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		FarBookChapters ).filter(
			FarBookChapters.deleted_at.is_( None ) ).all()

def softDeleteFarBookChapter( tgtMissingFarBookChapter, sesSource ):
	"""
		The list of FarBookChapters changes as time moves on, the FarBookChapters removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarBookChapterMissing():
		"""
			Determine that the farBookChapter object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsFarBookChapters._yyy_ == tgtMissingFarBookChapter._yyy_ ) )

		return not ret

	if flagFarBookChapterMissing():

		tgtMissingFarBookChapter.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarBookChapter

	else:
		raise TypeError('source person still exists and requires no soft delete!')

