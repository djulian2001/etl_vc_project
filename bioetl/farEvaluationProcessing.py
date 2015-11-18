import datetime
from sqlalchemy import exists, literal

from models.biopublicmodels import FarEvaluations
from asutobio.models.biopsmodels import BioPsFarEvaluations


def getSourceFarEvaluations( sesSource ):
	"""
		Isolate the imports for the ORM records into this file
		Returns the set of records from the FarEvaluations table of the source database.
	"""

	return sesSource.query( BioPsFarEvaluations ).all()

# change value to the singular
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

#template mapping... column where(s) _yyy_ 
	true, false = literal(True), literal(False)

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
					FarEvaluations.source_hash == srcFarEvaluation.source_hash ) )

			return not ret

		if farEvaluationUpdateRequired():
			# retrive the tables object to update.
			updateFarEvaluation = sesTarget.query(
				FarEvaluations ).filter(
					FarEvaluations.evaluationid == srcFarEvaluation.evaluationid ).filter(
					FarEvaluations.emplid == srcFarEvaluation.emplid ).one()

			# repeat the following pattern for all mapped attributes:
			updateFarEvaluation.source_hash = srcFarEvaluation.source_hash
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
			raise TypeError('source farEvaluation already exists and requires no updates!')

	else:
		insertFarEvaluation = FarEvaluations(
			source_hash = srcFarEvaluation.source_hash,
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

def softDeleteFarEvaluation( tgtMissingFarEvaluation, sesSource ):
	"""
		The list of FarEvaluations changes as time moves on, the FarEvaluations removed from the list are not
		deleted, but flaged removed by the deleted_at field.

		The return of this function returns a sqlalchemy object to update a person object.
	"""

	def flagFarEvaluationMissing():
		"""
			Determine that the farEvaluation object is still showing up in the source database.
			@True: If the data was not found and requires an update against the target database.
			@False: If the data was found and no action is required. 
		"""
		(ret, ), = sesSource.query(
			exists().where(
				FarEvaluations.evaluationid == tgtMissingFarEvaluation.evaluationid ).where(
				FarEvaluations.emplid == tgtMissingFarEvaluation.emplid ) )

		return not ret

	if flagFarEvaluationMissing():

		tgtMissingFarEvaluation.deleted_at = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )

		return tgtMissingFarEvaluation

	else:
		raise TypeError('source person still exists and requires no soft delete!')

