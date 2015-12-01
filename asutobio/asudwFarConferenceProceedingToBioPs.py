from models.asudwpsmodels import AsuDwPsFarConferenceProceedings, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarConferenceProceedings

from sharedProcesses import hashThisList

# the data pull
def getSourceFarConferenceProceedingsData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarConferenceProceedings model.
		@returns: the record set
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


# the data load
def processFarConferenceProceedingData( farConferenceProceeding ):
	"""
		Process an AsuDwPsFarConferenceProceedings object and prepare it for insert into the target BioPsFarConferenceProceedings table
		@return: the sa add object 
	"""
	
	farConferenceProceedingList = [
		farConferenceProceeding.conferenceproceedingid,
		farConferenceProceeding.src_sys_id,
		farConferenceProceeding.evaluationid,
		farConferenceProceeding.authors,
		farConferenceProceeding.title,
		farConferenceProceeding.journalname,
		farConferenceProceeding.refereed,
		farConferenceProceeding.publicationstatuscode,
		farConferenceProceeding.publicationyear,
		farConferenceProceeding.volumenumber,
		farConferenceProceeding.pages,
		farConferenceProceeding.webaddress,
		farConferenceProceeding.abstract,
		farConferenceProceeding.additionalinfo,
		farConferenceProceeding.dtcreated,
		farConferenceProceeding.dtupdated,
		farConferenceProceeding.userlastmodified,
		farConferenceProceeding.ispublic,
		farConferenceProceeding.activityid,
		farConferenceProceeding.load_error,
		farConferenceProceeding.data_origin,
		farConferenceProceeding.created_ew_dttm,
		farConferenceProceeding.lastupd_dw_dttm,
		farConferenceProceeding.batch_sid ]

	farConferenceProceedingHash = hashThisList( farConferenceProceedingList )

	tgtFarConferenceProceeding = BioPsFarConferenceProceedings(
		source_hash = farConferenceProceedingHash,
		conferenceproceedingid = farConferenceProceeding.conferenceproceedingid,
		src_sys_id = farConferenceProceeding.src_sys_id,
		evaluationid = farConferenceProceeding.evaluationid,
		authors = farConferenceProceeding.authors,
		title = farConferenceProceeding.title,
		journalname = farConferenceProceeding.journalname,
		refereed = farConferenceProceeding.refereed,
		publicationstatuscode = farConferenceProceeding.publicationstatuscode,
		publicationyear = farConferenceProceeding.publicationyear,
		volumenumber = farConferenceProceeding.volumenumber,
		pages = farConferenceProceeding.pages,
		webaddress = farConferenceProceeding.webaddress,
		abstract = farConferenceProceeding.abstract,
		additionalinfo = farConferenceProceeding.additionalinfo,
		dtcreated = farConferenceProceeding.dtcreated,
		dtupdated = farConferenceProceeding.dtupdated,
		userlastmodified = farConferenceProceeding.userlastmodified,
		ispublic = farConferenceProceeding.ispublic,
		activityid = farConferenceProceeding.activityid,
		load_error = farConferenceProceeding.load_error,
		data_origin = farConferenceProceeding.data_origin,
		created_ew_dttm = farConferenceProceeding.created_ew_dttm,
		lastupd_dw_dttm = farConferenceProceeding.lastupd_dw_dttm,
		batch_sid = farConferenceProceeding.batch_sid )

	return tgtFarConferenceProceeding
