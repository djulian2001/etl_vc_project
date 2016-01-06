import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarConferenceProceedings, FarEvaluations
from models.asudwpsmodels import AsuDwPsFarConferenceProceedings, AsuDwPsFarEvaluations, AsuPsBioFilters


def getSourceFarConferenceProceedings( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarConferenceProceedings table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarConferenceProceedings ).join(
			farEvals, AsuDwPsFarConferenceProceedings.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarConferenceProceedings.ispublic != 'N' ).all()


# change value to the singular
def processFarConferenceProceeding( srcFarConferenceProceeding, sesTarget ):
	"""
		Takes in a source FarConferenceProceeding object from biopsmodels (mysql.bio_ps.FarConferenceProceedings)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarConferenceProceedings), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farConferenceProceedingList = [
		srcFarConferenceProceeding.conferenceproceedingid,
		srcFarConferenceProceeding.src_sys_id,
		srcFarConferenceProceeding.evaluationid,
		srcFarConferenceProceeding.authors,
		srcFarConferenceProceeding.title,
		srcFarConferenceProceeding.journalname,
		srcFarConferenceProceeding.refereed,
		srcFarConferenceProceeding.publicationstatuscode,
		srcFarConferenceProceeding.publicationyear,
		srcFarConferenceProceeding.volumenumber,
		srcFarConferenceProceeding.pages,
		srcFarConferenceProceeding.webaddress,
		srcFarConferenceProceeding.abstract,
		srcFarConferenceProceeding.additionalinfo,
		srcFarConferenceProceeding.dtcreated,
		srcFarConferenceProceeding.dtupdated,
		srcFarConferenceProceeding.userlastmodified,
		srcFarConferenceProceeding.ispublic,
		srcFarConferenceProceeding.activityid,
		srcFarConferenceProceeding.load_error,
		srcFarConferenceProceeding.data_origin,
		srcFarConferenceProceeding.created_ew_dttm,
		srcFarConferenceProceeding.lastupd_dw_dttm,
		srcFarConferenceProceeding.batch_sid ]

	srcHash = hashThisList( farConferenceProceedingList )

	def farConferenceProceedingExists():
		"""
			determine the farConferenceProceeding exists in the target database.
			@True: The farConferenceProceeding exists in the database
			@False: The farConferenceProceeding does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ) )

		return ret

	if farConferenceProceedingExists():

		def farConferenceProceedingUpdateRequired():
			"""
				Determine if the farConferenceProceeding that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ).where(
					FarConferenceProceedings.source_hash == srcHash ).where(
					FarConferenceProceedings.deleted_at.is_( None ) ) )

			return not ret

		if farConferenceProceedingUpdateRequired():
			# retrive the tables object to update.
			updateFarConferenceProceeding = sesTarget.query(
				FarConferenceProceedings ).filter(
					FarConferenceProceedings.conferenceproceedingid == srcFarConferenceProceeding.conferenceproceedingid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarConferenceProceeding.source_hash = srcHash
			updateFarConferenceProceeding.conferenceproceedingid = srcFarConferenceProceeding.conferenceproceedingid
			updateFarConferenceProceeding.src_sys_id = srcFarConferenceProceeding.src_sys_id
			updateFarConferenceProceeding.evaluationid = srcFarConferenceProceeding.evaluationid
			updateFarConferenceProceeding.authors = srcFarConferenceProceeding.authors
			updateFarConferenceProceeding.title = srcFarConferenceProceeding.title
			updateFarConferenceProceeding.journalname = srcFarConferenceProceeding.journalname
			updateFarConferenceProceeding.refereed = srcFarConferenceProceeding.refereed
			updateFarConferenceProceeding.publicationstatuscode = srcFarConferenceProceeding.publicationstatuscode
			updateFarConferenceProceeding.publicationyear = srcFarConferenceProceeding.publicationyear
			updateFarConferenceProceeding.volumenumber = srcFarConferenceProceeding.volumenumber
			updateFarConferenceProceeding.pages = srcFarConferenceProceeding.pages
			updateFarConferenceProceeding.webaddress = srcFarConferenceProceeding.webaddress
			updateFarConferenceProceeding.abstract = srcFarConferenceProceeding.abstract
			updateFarConferenceProceeding.additionalinfo = srcFarConferenceProceeding.additionalinfo
			updateFarConferenceProceeding.dtcreated = srcFarConferenceProceeding.dtcreated
			updateFarConferenceProceeding.dtupdated = srcFarConferenceProceeding.dtupdated
			updateFarConferenceProceeding.userlastmodified = srcFarConferenceProceeding.userlastmodified
			updateFarConferenceProceeding.ispublic = srcFarConferenceProceeding.ispublic
			updateFarConferenceProceeding.activityid = srcFarConferenceProceeding.activityid
			updateFarConferenceProceeding.load_error = srcFarConferenceProceeding.load_error
			updateFarConferenceProceeding.data_origin = srcFarConferenceProceeding.data_origin
			updateFarConferenceProceeding.created_ew_dttm = srcFarConferenceProceeding.created_ew_dttm
			updateFarConferenceProceeding.lastupd_dw_dttm = srcFarConferenceProceeding.lastupd_dw_dttm
			updateFarConferenceProceeding.batch_sid = srcFarConferenceProceeding.batch_sid
			updateFarConferenceProceeding.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarConferenceProceeding.deleted_at = None

			return updateFarConferenceProceeding

	else:
		srcGetFarEvaluationId = sesTarget.query(
			FarEvaluations.id ).filter(
				FarEvaluations.evaluationid == srcFarConferenceProceeding.evaluationid ).one()

		insertFarConferenceProceeding = FarConferenceProceedings(
			source_hash = srcHash,
			far_evaluation_id = srcGetFarEvaluationId.id,
			conferenceproceedingid = srcFarConferenceProceeding.conferenceproceedingid,
			src_sys_id = srcFarConferenceProceeding.src_sys_id,
			evaluationid = srcFarConferenceProceeding.evaluationid,
			authors = srcFarConferenceProceeding.authors,
			title = srcFarConferenceProceeding.title,
			journalname = srcFarConferenceProceeding.journalname,
			refereed = srcFarConferenceProceeding.refereed,
			publicationstatuscode = srcFarConferenceProceeding.publicationstatuscode,
			publicationyear = srcFarConferenceProceeding.publicationyear,
			volumenumber = srcFarConferenceProceeding.volumenumber,
			pages = srcFarConferenceProceeding.pages,
			webaddress = srcFarConferenceProceeding.webaddress,
			abstract = srcFarConferenceProceeding.abstract,
			additionalinfo = srcFarConferenceProceeding.additionalinfo,
			dtcreated = srcFarConferenceProceeding.dtcreated,
			dtupdated = srcFarConferenceProceeding.dtupdated,
			userlastmodified = srcFarConferenceProceeding.userlastmodified,
			ispublic = srcFarConferenceProceeding.ispublic,
			activityid = srcFarConferenceProceeding.activityid,
			load_error = srcFarConferenceProceeding.load_error,
			data_origin = srcFarConferenceProceeding.data_origin,
			created_ew_dttm = srcFarConferenceProceeding.created_ew_dttm,
			lastupd_dw_dttm = srcFarConferenceProceeding.lastupd_dw_dttm,
			batch_sid = srcFarConferenceProceeding.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarConferenceProceeding

def getTargetFarConferenceProceedings( sesTarget ):
	"""
		Returns a set of FarConferenceProceedings objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarConferenceProceedings ).filter(
			FarConferenceProceedings.deleted_at.is_( None ) ).all()

def softDeleteFarConferenceProceeding( tgtRecord, srcRecords ):
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
		return not any( srcRecord.conferenceproceedingid == tgtRecord.conferenceproceedingid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
