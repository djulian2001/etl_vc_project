import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarBookReviews, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarBookReviews, AsuDwPsFarEvaluations, AsuPsBioFilters


def getSourceFarBookReviews( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarBookReviews table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarBookReviews ).join(
			farEvals, AsuDwPsFarBookReviews.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarBookReviews.ispublic !='N' ).all()
	
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

	farBookReviewList = [
		srcFarBookReview.bookreviewid,
		srcFarBookReview.src_sys_id,
		srcFarBookReview.evaluationid,
		srcFarBookReview.bookauthors,
		srcFarBookReview.booktitle,
		srcFarBookReview.journalname,
		srcFarBookReview.publicationstatuscode,
		srcFarBookReview.journalpages,
		srcFarBookReview.journalpublicationyear,
		srcFarBookReview.journalvolumenumber,
		srcFarBookReview.webaddress,
		srcFarBookReview.additionalinfo,
		srcFarBookReview.dtcreated,
		srcFarBookReview.dtupdated,
		srcFarBookReview.userlastmodified,
		srcFarBookReview.ispublic,
		srcFarBookReview.activityid,
		srcFarBookReview.load_error,
		srcFarBookReview.data_origin,
		srcFarBookReview.created_ew_dttm,
		srcFarBookReview.lastupd_dw_dttm,
		srcFarBookReview.batch_sid ]

	srcHash = hashThisList( farBookReviewList )

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
					FarBookReviews.source_hash == srcHash ).where(	
					FarBookReviews.deleted_at.is_( None ) ) )

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

def softDeleteFarBookReview( tgtRecord, srcRecords ):
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
		return not any( srcRecord.bookreviewid == tgtRecord.bookreviewid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')

