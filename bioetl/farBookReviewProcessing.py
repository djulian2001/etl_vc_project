import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarBookReviews, FarEvaluations
from asutobio.models.biopsmodels import BioPsFarBookReviews


def getSourceFarBookReviews( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarBookReviews table of the source database.
	"""

	return sesSource.query( BioPsFarBookReviews ).all()

# change value to the singular
def processFarBookReview( srcFarBookReview, sesTarget ):
	"""
		Takes in a source FarBookReview object from biopsmodels (mysql.bio_ps.FarBookReviews)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarBookReviews), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""

	true, false = literal(True), literal(False)

	def farBookReviewExists():
		"""
			determine the farBookReview exists in the target database.
			@True: The farBookReview exists in the database
			@False: The farBookReview does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarBookReviews.bookreviewid == srcFarBookReview.bookreviewid ) )

		return ret

	if farBookReviewExists():

		def farBookReviewUpdateRequired():
			"""
				Determine if the farBookReview that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarBookReviews.bookreviewid == srcFarBookReview.bookreviewid ).where(
					FarBookReviews.source_hash == srcFarBookReview.source_hash ) )

			return not ret

		if farBookReviewUpdateRequired():
			# retrive the tables object to update.
			updateFarBookReview = sesTarget.query(
				FarBookReviews ).filter(
					FarBookReviews.bookreviewid == srcFarBookReview.bookreviewid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarBookReview.source_hash = srcFarBookReview.source_hash
			updateFarBookReview.bookreviewid = srcFarBookReview.bookreviewid
			updateFarBookReview.src_sys_id = srcFarBookReview.src_sys_id
			updateFarBookReview.evaluationid = srcFarBookReview.evaluationid
			updateFarBookReview.bookauthors = srcFarBookReview.bookauthors
			updateFarBookReview.booktitle = srcFarBookReview.booktitle
			updateFarBookReview.journalname = srcFarBookReview.journalname
			updateFarBookReview.publicationstatuscode = srcFarBookReview.publicationstatuscode
			updateFarBookReview.journalpages = srcFarBookReview.journalpages
			updateFarBookReview.journalpublicationyear = srcFarBookReview.journalpublicationyear
			updateFarBookReview.journalvolumenumber = srcFarBookReview.journalvolumenumber
			updateFarBookReview.webaddress = srcFarBookReview.webaddress
			updateFarBookReview.additionalinfo = srcFarBookReview.additionalinfo
			updateFarBookReview.dtcreated = srcFarBookReview.dtcreated
			updateFarBookReview.dtupdated = srcFarBookReview.dtupdated
			updateFarBookReview.userlastmodified = srcFarBookReview.userlastmodified
			updateFarBookReview.ispublic = srcFarBookReview.ispublic
			updateFarBookReview.activityid = srcFarBookReview.activityid
			updateFarBookReview.load_error = srcFarBookReview.load_error
			updateFarBookReview.data_origin = srcFarBookReview.data_origin
			updateFarBookReview.created_ew_dttm = srcFarBookReview.created_ew_dttm
			updateFarBookReview.lastupd_dw_dttm = srcFarBookReview.lastupd_dw_dttm
			updateFarBookReview.batch_sid = srcFarBookReview.batch_sid
			updateFarBookReview.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarBookReview.deleted_at = None

			return updateFarBookReview
		else:
			raise TypeError('source farBookReview already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarBookReview.evaluationid ).one()

		insertFarBookReview = FarBookReviews(
			source_hash = srcFarBookReview.source_hash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			bookreviewid = srcFarBookReview.bookreviewid,
			src_sys_id = srcFarBookReview.src_sys_id,
			evaluationid = srcFarBookReview.evaluationid,
			bookauthors = srcFarBookReview.bookauthors,
			booktitle = srcFarBookReview.booktitle,
			journalname = srcFarBookReview.journalname,
			publicationstatuscode = srcFarBookReview.publicationstatuscode,
			journalpages = srcFarBookReview.journalpages,
			journalpublicationyear = srcFarBookReview.journalpublicationyear,
			journalvolumenumber = srcFarBookReview.journalvolumenumber,
			webaddress = srcFarBookReview.webaddress,
			additionalinfo = srcFarBookReview.additionalinfo,
			dtcreated = srcFarBookReview.dtcreated,
			dtupdated = srcFarBookReview.dtupdated,
			userlastmodified = srcFarBookReview.userlastmodified,
			ispublic = srcFarBookReview.ispublic,
			activityid = srcFarBookReview.activityid,
			load_error = srcFarBookReview.load_error,
			data_origin = srcFarBookReview.data_origin,
			created_ew_dttm = srcFarBookReview.created_ew_dttm,
			lastupd_dw_dttm = srcFarBookReview.lastupd_dw_dttm,
			batch_sid = srcFarBookReview.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarBookReview

def getTargetFarBookReviews( sesTarget ):
	"""
		Returns a set of FarBookReviews objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		FarBookReviews ).filter(
			FarBookReviews.deleted_at.is_( None ) ).all()

def softDeleteFarBookReview( tgtMissingFarBookReview, sesSource ):
	"""
		The list of FarBookReviews changes as time moves on, the FarBookReviews removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarBookReviewMissing():
		"""
			Determine that the farBookReview object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				BioPsFarBookReviews.bookreviewid == tgtMissingFarBookReview.bookreviewid ) )

		return not ret

	if flagFarBookReviewMissing():

		tgtMissingFarBookReview.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarBookReview

	else:
		raise TypeError('source person still exists and requires no soft delete!')

