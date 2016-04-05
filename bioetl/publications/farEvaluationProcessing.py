import datetime
from sqlalchemy import exists, literal

from sharedProcesses import hashThisList
from models.biopublicmodels import FarEvaluations, People
from models.asudwpsmodels import AsuDwPsFarEvaluations, AsuPsBioFilters

def getSourceFarEvaluations( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarEvaluations table of the source database.
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsFarEvaluations ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).order_by(
				AsuDwPsFarEvaluations.emplid ).all()

def processFarEvaluation( srcFarEvaluation, sesTarget ):
	"""
		Takes in a source FarEvaluation object from biopsmodels (mysql.bio_ps.FarEvaluations)
		and determines if the object needs to be updated, inserted in the target
		database (mysql.bio_public.FarEvaluations), or that nothing needs doing.
	
		Selecting Booleans from the databases.  Using conjunctions to make the exists()
		a boolean return from the query() method.  Bit more syntax but a sqlalchemy object
		returned will not be truthy/falsey.
		(http://techspot.zzzeek.org/2008/09/09/selecting-booleans/)
	"""
	true, false = literal(True), literal(False)

	farEvaluationList = [
		srcFarEvaluation.evaluationid,
		srcFarEvaluation.src_sys_id,
		srcFarEvaluation.calendaryear,
		srcFarEvaluation.emplid,
		srcFarEvaluation.asuriteid,
		srcFarEvaluation.asuid,
		srcFarEvaluation.faculty_rank_title,
		srcFarEvaluation.job_title,
		srcFarEvaluation.tenure_status_code,
		srcFarEvaluation.tenurehomedeptcode,
		srcFarEvaluation.extensiondate,
		srcFarEvaluation.completed,
		srcFarEvaluation.dtcreated,
		srcFarEvaluation.dtupdated,
		srcFarEvaluation.userlastmodified,
		srcFarEvaluation.load_error,
		srcFarEvaluation.data_origin,
		srcFarEvaluation.created_ew_dttm,
		srcFarEvaluation.lastupd_dw_dttm,
		srcFarEvaluation.batch_sid ]

	srcHash = hashThisList( farEvaluationList )

	def farEvaluationExists():
		"""
			determine the farEvaluation exists in the target database.
			@True: The farEvaluation exists in the database
			@False: The farEvaluation does not exist in the database
		"""
		(ret, ), = sesTarget.query(
			exists().where(
				FarEvaluations.evaluationid == srcFarEvaluation.evaluationid ).where(
				FarEvaluations.emplid == srcFarEvaluation.emplid ) )

		return ret

	if farEvaluationExists():

		def farEvaluationUpdateRequired():
			"""
				Determine if the farEvaluation that exists requires and update.
				@True: returned if source_hash is unchanged
				@False: returned if source_hash is different
			"""	
			(ret, ), = sesTarget.query(
				exists().where(
					FarEvaluations.evaluationid == srcFarEvaluation.evaluationid ).where(
					FarEvaluations.emplid == srcFarEvaluation.emplid ).where(
					FarEvaluations.source_hash == srcHash ).where(
					FarEvaluations.deleted_at.is_( None ) ) )

			return not ret

		if farEvaluationUpdateRequired():
			# retrive the tables object to update.
			updateFarEvaluation = sesTarget.query(
				FarEvaluations ).filter(
					FarEvaluations.evaluationid == srcFarEvaluation.evaluationid ).filter(
					FarEvaluations.emplid == srcFarEvaluation.emplid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarEvaluation.source_hash = srcHash
			updateFarEvaluation.evaluationid = srcFarEvaluation.evaluationid
			updateFarEvaluation.src_sys_id = srcFarEvaluation.src_sys_id
			updateFarEvaluation.calendaryear = srcFarEvaluation.calendaryear
			updateFarEvaluation.emplid = srcFarEvaluation.emplid
			updateFarEvaluation.asuriteid = srcFarEvaluation.asuriteid
			updateFarEvaluation.asuid = srcFarEvaluation.asuid
			updateFarEvaluation.faculty_rank_title = srcFarEvaluation.faculty_rank_title
			updateFarEvaluation.job_title = srcFarEvaluation.job_title
			updateFarEvaluation.tenure_status_code = srcFarEvaluation.tenure_status_code
			updateFarEvaluation.tenurehomedeptcode = srcFarEvaluation.tenurehomedeptcode
			updateFarEvaluation.extensiondate = srcFarEvaluation.extensiondate
			updateFarEvaluation.completed = srcFarEvaluation.completed
			updateFarEvaluation.dtcreated = srcFarEvaluation.dtcreated
			updateFarEvaluation.dtupdated = srcFarEvaluation.dtupdated
			updateFarEvaluation.userlastmodified = srcFarEvaluation.userlastmodified
			updateFarEvaluation.load_error = srcFarEvaluation.load_error
			updateFarEvaluation.data_origin = srcFarEvaluation.data_origin
			updateFarEvaluation.created_ew_dttm = srcFarEvaluation.created_ew_dttm
			updateFarEvaluation.lastupd_dw_dttm = srcFarEvaluation.lastupd_dw_dttm
			updateFarEvaluation.batch_sid = srcFarEvaluation.batch_sid

			updateFarEvaluation.updated_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
			updateFarEvaluation.deleted_at = None

			return updateFarEvaluation

	else:
		srcGetPersonId = sesTarget.query(
			People.id ).filter(
				People.emplid == srcFarEvaluation.emplid ).one()

		insertFarEvaluation = FarEvaluations(
			person_id = srcGetPersonId.id,
			source_hash = srcHash,
			evaluationid = srcFarEvaluation.evaluationid,
			src_sys_id = srcFarEvaluation.src_sys_id,
			calendaryear = srcFarEvaluation.calendaryear,
			emplid = srcFarEvaluation.emplid,
			asuriteid = srcFarEvaluation.asuriteid,
			asuid = srcFarEvaluation.asuid,
			faculty_rank_title = srcFarEvaluation.faculty_rank_title,
			job_title = srcFarEvaluation.job_title,
			tenure_status_code = srcFarEvaluation.tenure_status_code,
			tenurehomedeptcode = srcFarEvaluation.tenurehomedeptcode,
			extensiondate = srcFarEvaluation.extensiondate,
			completed = srcFarEvaluation.completed,
			dtcreated = srcFarEvaluation.dtcreated,
			dtupdated = srcFarEvaluation.dtupdated,
			userlastmodified = srcFarEvaluation.userlastmodified,
			load_error = srcFarEvaluation.load_error,
			data_origin = srcFarEvaluation.data_origin,
			created_ew_dttm = srcFarEvaluation.created_ew_dttm,
			lastupd_dw_dttm = srcFarEvaluation.lastupd_dw_dttm,
			batch_sid = srcFarEvaluation.batch_sid,
			created_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' ) )

		return insertFarEvaluation

def getTargetFarEvaluations( sesTarget ):
	"""
		Returns a set of FarEvaluations objects from the target database where the records are not flagged
		deleted_at.
	"""
	return sesTarget.query(
		FarEvaluations ).filter(
			FarEvaluations.deleted_at.is_( None ) ).all()

def softDeleteFarEvaluation( tgtRecord, srcRecords ):
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
		return not any( srcRecord.emplid == tgtRecord.emplid and srcRecord.evaluationid == tgtRecord.evaluationid for srcRecord in srcRecords )

	if dataMissing():
		tgtRecord.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
		return tgtRecord
