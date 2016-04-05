import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarBookChapters, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarBookChapters, AsuDwPsFarEvaluations, AsuPsBioFilters


def getSourceFarBookChapters( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarBookChapters table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarBookChapters ).join(
			farEvals, AsuDwPsFarBookChapters.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarBookChapters.ispublic !='N' ).all()
	

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
	true, false = literal(True), literal(False)

	farBookChapterList = [
		srcFarBookChapter.bookchapterid,
		srcFarBookChapter.src_sys_id,
		srcFarBookChapter.evaluationid,
		srcFarBookChapter.bookauthors,
		srcFarBookChapter.booktitle,
		srcFarBookChapter.chapterauthors,
		srcFarBookChapter.chaptertitle,
		srcFarBookChapter.publisher,
		srcFarBookChapter.publicationstatuscode,
		srcFarBookChapter.pages,
		srcFarBookChapter.isbn,
		srcFarBookChapter.publicationyear,
		srcFarBookChapter.volumenumber,
		srcFarBookChapter.edition,
		srcFarBookChapter.publicationcity,
		srcFarBookChapter.webaddress,
		srcFarBookChapter.translated,
		srcFarBookChapter.additionalinfo,
		srcFarBookChapter.dtcreated,
		srcFarBookChapter.dtupdated,
		srcFarBookChapter.userlastmodified,
		srcFarBookChapter.ispublic,
		srcFarBookChapter.activityid,
		srcFarBookChapter.load_error,
		srcFarBookChapter.data_origin,
		srcFarBookChapter.created_ew_dttm,
		srcFarBookChapter.lastupd_dw_dttm,
		srcFarBookChapter.batch_sid ]

	srcHash = hashThisList( farBookChapterList )

	def farBookChapterExists():
		"""
			determine the farBookChapter exists in the target database.
			@True: The farBookChapter exists in the database
			@False: The farBookChapter does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarBookChapters.bookchapterid == srcFarBookChapter.bookchapterid ) )

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
					FarBookChapters.bookchapterid == srcFarBookChapter.bookchapterid ).where(
					FarBookChapters.source_hash == srcHash ).where(	
					FarBookChapters.deleted_at.is_( None ) ) )

			return not ret

		if farBookChapterUpdateRequired():
			# retrive the tables object to update.
			updateFarBookChapter = sesTarget.query(
				FarBookChapters ).filter(
					FarBookChapters.bookchapterid == srcFarBookChapter.bookchapterid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarBookChapter.source_hash = srcHash
			updateFarBookChapter.bookchapterid = srcFarBookChapter.bookchapterid
			updateFarBookChapter.src_sys_id = srcFarBookChapter.src_sys_id
			updateFarBookChapter.evaluationid = srcFarBookChapter.evaluationid
			updateFarBookChapter.bookauthors = srcFarBookChapter.bookauthors
			updateFarBookChapter.booktitle = srcFarBookChapter.booktitle
			updateFarBookChapter.chapterauthors = srcFarBookChapter.chapterauthors
			updateFarBookChapter.chaptertitle = srcFarBookChapter.chaptertitle
			updateFarBookChapter.publisher = srcFarBookChapter.publisher
			updateFarBookChapter.publicationstatuscode = srcFarBookChapter.publicationstatuscode
			updateFarBookChapter.pages = srcFarBookChapter.pages
			updateFarBookChapter.isbn = srcFarBookChapter.isbn
			updateFarBookChapter.publicationyear = srcFarBookChapter.publicationyear
			updateFarBookChapter.volumenumber = srcFarBookChapter.volumenumber
			updateFarBookChapter.edition = srcFarBookChapter.edition
			updateFarBookChapter.publicationcity = srcFarBookChapter.publicationcity
			updateFarBookChapter.webaddress = srcFarBookChapter.webaddress
			updateFarBookChapter.translated = srcFarBookChapter.translated
			updateFarBookChapter.additionalinfo = srcFarBookChapter.additionalinfo
			updateFarBookChapter.dtcreated = srcFarBookChapter.dtcreated
			updateFarBookChapter.dtupdated = srcFarBookChapter.dtupdated
			updateFarBookChapter.userlastmodified = srcFarBookChapter.userlastmodified
			updateFarBookChapter.ispublic = srcFarBookChapter.ispublic
			updateFarBookChapter.activityid = srcFarBookChapter.activityid
			updateFarBookChapter.load_error = srcFarBookChapter.load_error
			updateFarBookChapter.data_origin = srcFarBookChapter.data_origin
			updateFarBookChapter.created_ew_dttm = srcFarBookChapter.created_ew_dttm
			updateFarBookChapter.lastupd_dw_dttm = srcFarBookChapter.lastupd_dw_dttm
			updateFarBookChapter.batch_sid = srcFarBookChapter.batch_sid
			updateFarBookChapter.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarBookChapter.deleted_at = None

			return updateFarBookChapter

	else:
		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarBookChapter.evaluationid ).one()

		insertFarBookChapter = FarBookChapters(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			bookchapterid = srcFarBookChapter.bookchapterid,
			src_sys_id = srcFarBookChapter.src_sys_id,
			evaluationid = srcFarBookChapter.evaluationid,
			bookauthors = srcFarBookChapter.bookauthors,
			booktitle = srcFarBookChapter.booktitle,
			chapterauthors = srcFarBookChapter.chapterauthors,
			chaptertitle = srcFarBookChapter.chaptertitle,
			publisher = srcFarBookChapter.publisher,
			publicationstatuscode = srcFarBookChapter.publicationstatuscode,
			pages = srcFarBookChapter.pages,
			isbn = srcFarBookChapter.isbn,
			publicationyear = srcFarBookChapter.publicationyear,
			volumenumber = srcFarBookChapter.volumenumber,
			edition = srcFarBookChapter.edition,
			publicationcity = srcFarBookChapter.publicationcity,
			webaddress = srcFarBookChapter.webaddress,
			translated = srcFarBookChapter.translated,
			additionalinfo = srcFarBookChapter.additionalinfo,
			dtcreated = srcFarBookChapter.dtcreated,
			dtupdated = srcFarBookChapter.dtupdated,
			userlastmodified = srcFarBookChapter.userlastmodified,
			ispublic = srcFarBookChapter.ispublic,
			activityid = srcFarBookChapter.activityid,
			load_error = srcFarBookChapter.load_error,
			data_origin = srcFarBookChapter.data_origin,
			created_ew_dttm = srcFarBookChapter.created_ew_dttm,
			lastupd_dw_dttm = srcFarBookChapter.lastupd_dw_dttm,
			batch_sid = srcFarBookChapter.batch_sid,
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

def softDeleteFarBookChapter( tgtRecord, srcRecords ):
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
		return not any( srcRecord.bookchapterid == tgtRecord.bookchapterid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
