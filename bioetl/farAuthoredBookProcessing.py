import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarAuthoredBooks, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarAuthoredBooks, AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarAuthoredBooks( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarAuthoredBooks table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarAuthoredBooks ).join(
			farEvals, AsuDwPsFarAuthoredBooks.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarAuthoredBooks.ispublic !='N' ).all()
		

# change value to the singular
def processFarAuthoredBook( srcFarAuthoredBook, sesTarget ):
	"""
		Takes in a source FarAuthoredBook object from biopsmodels (mysql.bio_ps.FarAuthoredBooks)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarAuthoredBooks), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farAuthoredBookList = [
		srcFarAuthoredBook.authoredbookid,
		srcFarAuthoredBook.src_sys_id,
		srcFarAuthoredBook.evaluationid,
		srcFarAuthoredBook.authors,
		srcFarAuthoredBook.title,
		srcFarAuthoredBook.publisher,
		srcFarAuthoredBook.publicationstatuscode,
		srcFarAuthoredBook.pages,
		srcFarAuthoredBook.isbn,
		srcFarAuthoredBook.publicationyear,
		srcFarAuthoredBook.volumenumber,
		srcFarAuthoredBook.edition,
		srcFarAuthoredBook.publicationcity,
		srcFarAuthoredBook.webaddress,
		srcFarAuthoredBook.translated,
		srcFarAuthoredBook.additionalinfo,
		srcFarAuthoredBook.dtcreated,
		srcFarAuthoredBook.dtupdated,
		srcFarAuthoredBook.userlastmodified,
		srcFarAuthoredBook.ispublic,
		srcFarAuthoredBook.activityid,
		srcFarAuthoredBook.load_error,
		srcFarAuthoredBook.data_origin,
		srcFarAuthoredBook.created_ew_dttm,
		srcFarAuthoredBook.lastupd_dw_dttm,
		srcFarAuthoredBook.batch_sid ]

	srcHash = hashThisList( farAuthoredBookList )

	def farAuthoredBookExists():
		"""
			determine the farAuthoredBook exists in the target database.
			@True: The farAuthoredBook exists in the database
			@False: The farAuthoredBook does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarAuthoredBooks.authoredbookid == srcFarAuthoredBook.authoredbookid ) )

		return ret

	if farAuthoredBookExists():

		def farAuthoredBookUpdateRequired():
			"""
				Determine if the farAuthoredBook that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarAuthoredBooks.authoredbookid == srcFarAuthoredBook.authoredbookid ).where(
					FarAuthoredBooks.source_hash == srcHash ).where(
					FarAuthoredBooks.deleted_at.is_( None ) ) )

			return not ret

		if farAuthoredBookUpdateRequired():
			# retrive the tables object to update.
			updateFarAuthoredBook = sesTarget.query(
				FarAuthoredBooks ).filter(
					FarAuthoredBooks.authoredbookid == srcFarAuthoredBook.authoredbookid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarAuthoredBook.source_hash = srcHash
			updateFarAuthoredBook.authoredbookid = srcFarAuthoredBook.authoredbookid
			updateFarAuthoredBook.src_sys_id = srcFarAuthoredBook.src_sys_id
			updateFarAuthoredBook.evaluationid = srcFarAuthoredBook.evaluationid
			updateFarAuthoredBook.authors = srcFarAuthoredBook.authors
			updateFarAuthoredBook.title = srcFarAuthoredBook.title
			updateFarAuthoredBook.publisher = srcFarAuthoredBook.publisher
			updateFarAuthoredBook.publicationstatuscode = srcFarAuthoredBook.publicationstatuscode
			updateFarAuthoredBook.pages = srcFarAuthoredBook.pages
			updateFarAuthoredBook.isbn = srcFarAuthoredBook.isbn
			updateFarAuthoredBook.publicationyear = srcFarAuthoredBook.publicationyear
			updateFarAuthoredBook.volumenumber = srcFarAuthoredBook.volumenumber
			updateFarAuthoredBook.edition = srcFarAuthoredBook.edition
			updateFarAuthoredBook.publicationcity = srcFarAuthoredBook.publicationcity
			updateFarAuthoredBook.webaddress = srcFarAuthoredBook.webaddress
			updateFarAuthoredBook.translated = srcFarAuthoredBook.translated
			updateFarAuthoredBook.additionalinfo = srcFarAuthoredBook.additionalinfo
			updateFarAuthoredBook.dtcreated = srcFarAuthoredBook.dtcreated
			updateFarAuthoredBook.dtupdated = srcFarAuthoredBook.dtupdated
			updateFarAuthoredBook.userlastmodified = srcFarAuthoredBook.userlastmodified
			updateFarAuthoredBook.ispublic = srcFarAuthoredBook.ispublic
			updateFarAuthoredBook.activityid = srcFarAuthoredBook.activityid
			updateFarAuthoredBook.load_error = srcFarAuthoredBook.load_error
			updateFarAuthoredBook.data_origin = srcFarAuthoredBook.data_origin
			updateFarAuthoredBook.created_ew_dttm = srcFarAuthoredBook.created_ew_dttm
			updateFarAuthoredBook.lastupd_dw_dttm = srcFarAuthoredBook.lastupd_dw_dttm
			updateFarAuthoredBook.batch_sid = srcFarAuthoredBook.batch_sid
			updateFarAuthoredBook.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarAuthoredBook.deleted_at = None

			return updateFarAuthoredBook
		else:
			raise TypeError('source farAuthoredBook already exists and requires no updates!')

	else:
		
		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarAuthoredBook.evaluationid ).one()

		insertFarAuthoredBook = FarAuthoredBooks(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			authoredbookid = srcFarAuthoredBook.authoredbookid,
			src_sys_id = srcFarAuthoredBook.src_sys_id,
			evaluationid = srcFarAuthoredBook.evaluationid,
			authors = srcFarAuthoredBook.authors,
			title = srcFarAuthoredBook.title,
			publisher = srcFarAuthoredBook.publisher,
			publicationstatuscode = srcFarAuthoredBook.publicationstatuscode,
			pages = srcFarAuthoredBook.pages,
			isbn = srcFarAuthoredBook.isbn,
			publicationyear = srcFarAuthoredBook.publicationyear,
			volumenumber = srcFarAuthoredBook.volumenumber,
			edition = srcFarAuthoredBook.edition,
			publicationcity = srcFarAuthoredBook.publicationcity,
			webaddress = srcFarAuthoredBook.webaddress,
			translated = srcFarAuthoredBook.translated,
			additionalinfo = srcFarAuthoredBook.additionalinfo,
			dtcreated = srcFarAuthoredBook.dtcreated,
			dtupdated = srcFarAuthoredBook.dtupdated,
			userlastmodified = srcFarAuthoredBook.userlastmodified,
			ispublic = srcFarAuthoredBook.ispublic,
			activityid = srcFarAuthoredBook.activityid,
			load_error = srcFarAuthoredBook.load_error,
			data_origin = srcFarAuthoredBook.data_origin,
			created_ew_dttm = srcFarAuthoredBook.created_ew_dttm,
			lastupd_dw_dttm = srcFarAuthoredBook.lastupd_dw_dttm,
			batch_sid = srcFarAuthoredBook.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarAuthoredBook

def getTargetFarAuthoredBooks( sesTarget ):
	"""
		Returns a set of FarAuthoredBooks objects from the target database where the records are not flagged
		deleted_at.
	"""

	return sesTarget.query(
		FarAuthoredBooks ).filter(
			FarAuthoredBooks.deleted_at.is_( None ) ).all()

def softDeleteFarAuthoredBook( tgtRecord, srcRecords ):
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
		return not any( srcRecord.authoredbookid == tgtRecord.authoredbookid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')

