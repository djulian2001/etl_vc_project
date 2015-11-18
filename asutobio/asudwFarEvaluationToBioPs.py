from models.asudwpsmodels import AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarEvaluations

from sharedProcesses import hashThisList

# the data pull
def getSourceFarEvaluationsData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarEvaluations model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	return sesSource.query(
		AsuDwPsFarEvaluations ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid==srcEmplidsSubQry.c.emplid).order_by(
				AsuDwPsFarEvaluations.emplid).all()

# the data load
def processFarEvaluationData( farEvaluation ):
	"""
		Process an AsuDwPsFarEvaluations object and prepare it for insert into the target BioPsFarEvaluations table
		@return: the sa add object 
	"""
	
	farEvaluationList = [
		farEvaluation.evaluationid,
		farEvaluation.src_sys_id,
		farEvaluation.calendaryear,
		farEvaluation.emplid,
		farEvaluation.asuriteid,
		farEvaluation.asuid,
		farEvaluation.faculty_rank_title,
		farEvaluation.job_title,
		farEvaluation.tenure_status_code,
		farEvaluation.tenurehomedeptcode,
		farEvaluation.extensiondate,
		farEvaluation.completed,
		farEvaluation.dtcreated,
		farEvaluation.dtupdated,
		farEvaluation.userlastmodified,
		farEvaluation.load_error,
		farEvaluation.data_origin,
		farEvaluation.created_ew_dttm,
		farEvaluation.lastupd_dw_dttm,
		farEvaluation.batch_sid ]

	farEvaluationHash = hashThisList( farEvaluationList )

	tgtFarEvaluation = BioPsFarEvaluations(
		source_hash = farEvaluationHash,
		evaluationid = farEvaluation.evaluationid,
		src_sys_id = farEvaluation.src_sys_id,
		calendaryear = farEvaluation.calendaryear,
		emplid = farEvaluation.emplid,
		asuriteid = farEvaluation.asuriteid,
		asuid = farEvaluation.asuid,
		faculty_rank_title = farEvaluation.faculty_rank_title,
		job_title = farEvaluation.job_title,
		tenure_status_code = farEvaluation.tenure_status_code,
		tenurehomedeptcode = farEvaluation.tenurehomedeptcode,
		extensiondate = farEvaluation.extensiondate,
		completed = farEvaluation.completed,
		dtcreated = farEvaluation.dtcreated,
		dtupdated = farEvaluation.dtupdated,
		userlastmodified = farEvaluation.userlastmodified,
		load_error = farEvaluation.load_error,
		data_origin = farEvaluation.data_origin,
		created_ew_dttm = farEvaluation.created_ew_dttm,
		lastupd_dw_dttm = farEvaluation.lastupd_dw_dttm,
		batch_sid = farEvaluation.batch_sid )

	return tgtFarEvaluation

