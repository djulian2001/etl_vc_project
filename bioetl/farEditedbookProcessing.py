import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarEditedbooks, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarEditedbooks, AsuDwPsFarEvaluations, AsuPsBioFilters


def getSourceFarEditedbooks( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarEditedbooks table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarEditedbooks ).join(
			farEvals, AsuDwPsFarEditedbooks.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarEditedbooks.ispublic !='N' ).all()
	
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
	true, false = literal(True), literal(False)

	farEditedbookList = [
		srcFarEditedbook.editedbookid,
		srcFarEditedbook.src_sys_id,
		srcFarEditedbook.evaluationid,
		srcFarEditedbook.authors,
		srcFarEditedbook.title,
		srcFarEditedbook.publisher,
		srcFarEditedbook.publicationstatuscode,
		srcFarEditedbook.pages,
		srcFarEditedbook.isbn,
		srcFarEditedbook.publicationyear,
		srcFarEditedbook.volumenumber,
		srcFarEditedbook.edition,
		srcFarEditedbook.publicationcity,
		srcFarEditedbook.webaddress,
		srcFarEditedbook.translated,
		srcFarEditedbook.additionalinfo,
		srcFarEditedbook.dtcreated,
		srcFarEditedbook.dtupdated,
		srcFarEditedbook.userlastmodified,
		srcFarEditedbook.ispublic,
		srcFarEditedbook.activityid,
		srcFarEditedbook.load_error,
		srcFarEditedbook.data_origin,
		srcFarEditedbook.created_ew_dttm,
		srcFarEditedbook.lastupd_dw_dttm,
		srcFarEditedbook.batch_sid ]

	srcHash = hashThisList( farEditedbookList )

	def farEditedbookExists():
		"""
			determine the farEditedbook exists in the target database.
			@True: The farEditedbook exists in the database
			@False: The farEditedbook does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarEditedbooks.editedbookid == srcFarEditedbook.editedbookid ) )

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
					FarEditedbooks.editedbookid == srcFarEditedbook.editedbookid ).where(
					FarEditedbooks.source_hash == srcHash ).where(	
					FarEditedbooks.deleted_at.is_( None ) ) )

			return not ret

		if farEditedbookUpdateRequired():
			# retrive the tables object to update.
			updateFarEditedbook = sesTarget.query(
				FarEditedbooks ).filter(
					FarEditedbooks.editedbookid == srcFarEditedbook.editedbookid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarEditedbook.source_hash = srcHash
			updateFarEditedbook.editedbookid = srcFarEditedbook.editedbookid
			updateFarEditedbook.src_sys_id = srcFarEditedbook.src_sys_id
			updateFarEditedbook.evaluationid = srcFarEditedbook.evaluationid
			updateFarEditedbook.authors = srcFarEditedbook.authors
			updateFarEditedbook.title = srcFarEditedbook.title
			updateFarEditedbook.publisher = srcFarEditedbook.publisher
			updateFarEditedbook.publicationstatuscode = srcFarEditedbook.publicationstatuscode
			updateFarEditedbook.pages = srcFarEditedbook.pages
			updateFarEditedbook.isbn = srcFarEditedbook.isbn
			updateFarEditedbook.publicationyear = srcFarEditedbook.publicationyear
			updateFarEditedbook.volumenumber = srcFarEditedbook.volumenumber
			updateFarEditedbook.edition = srcFarEditedbook.edition
			updateFarEditedbook.publicationcity = srcFarEditedbook.publicationcity
			updateFarEditedbook.webaddress = srcFarEditedbook.webaddress
			updateFarEditedbook.translated = srcFarEditedbook.translated
			updateFarEditedbook.additionalinfo = srcFarEditedbook.additionalinfo
			updateFarEditedbook.dtcreated = srcFarEditedbook.dtcreated
			updateFarEditedbook.dtupdated = srcFarEditedbook.dtupdated
			updateFarEditedbook.userlastmodified = srcFarEditedbook.userlastmodified
			updateFarEditedbook.ispublic = srcFarEditedbook.ispublic
			updateFarEditedbook.activityid = srcFarEditedbook.activityid
			updateFarEditedbook.load_error = srcFarEditedbook.load_error
			updateFarEditedbook.data_origin = srcFarEditedbook.data_origin
			updateFarEditedbook.created_ew_dttm = srcFarEditedbook.created_ew_dttm
			updateFarEditedbook.lastupd_dw_dttm = srcFarEditedbook.lastupd_dw_dttm
			updateFarEditedbook.batch_sid = srcFarEditedbook.batch_sid
			updateFarEditedbook.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarEditedbook.deleted_at = None

			return updateFarEditedbook
		else:
			raise TypeError('source farEditedbook already exists and requires no updates!')

	else:

		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarEditedbook.evaluationid ).one()
	
		insertFarEditedbook = FarEditedbooks(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			editedbookid = srcFarEditedbook.editedbookid,
			src_sys_id = srcFarEditedbook.src_sys_id,
			evaluationid = srcFarEditedbook.evaluationid,
			authors = srcFarEditedbook.authors,
			title = srcFarEditedbook.title,
			publisher = srcFarEditedbook.publisher,
			publicationstatuscode = srcFarEditedbook.publicationstatuscode,
			pages = srcFarEditedbook.pages,
			isbn = srcFarEditedbook.isbn,
			publicationyear = srcFarEditedbook.publicationyear,
			volumenumber = srcFarEditedbook.volumenumber,
			edition = srcFarEditedbook.edition,
			publicationcity = srcFarEditedbook.publicationcity,
			webaddress = srcFarEditedbook.webaddress,
			translated = srcFarEditedbook.translated,
			additionalinfo = srcFarEditedbook.additionalinfo,
			dtcreated = srcFarEditedbook.dtcreated,
			dtupdated = srcFarEditedbook.dtupdated,
			userlastmodified = srcFarEditedbook.userlastmodified,
			ispublic = srcFarEditedbook.ispublic,
			activityid = srcFarEditedbook.activityid,
			load_error = srcFarEditedbook.load_error,
			data_origin = srcFarEditedbook.data_origin,
			created_ew_dttm = srcFarEditedbook.created_ew_dttm,
			lastupd_dw_dttm = srcFarEditedbook.lastupd_dw_dttm,
			batch_sid = srcFarEditedbook.batch_sid,
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

def softDeleteFarEditedbook( tgtRecord, srcRecords ):
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
		return not any( srcRecord.editedbookid == tgtRecord.editedbookid for srcRecord in srcRecords )


	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
	else:
		raise TypeError('source target record still exists and requires no soft delete!')
