from models.asudwpsmodels import AsuDwPsFarEditedbooks, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarEditedbooks

from sharedProcesses import hashThisList

# the data pull
def getSourceFarEditedbooksData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarEditedbooks model.
		@returns: the record set
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
	
# the data load
def processFarEditedbookData( farEditedbook ):
	"""
		Process an AsuDwPsFarEditedbooks object and prepare it for insert into the target BioPsFarEditedbooks table
		@return: the sa add object 
	"""
	
	farEditedbookList = [
		farEditedbook.editedbookid,
		farEditedbook.src_sys_id,
		farEditedbook.evaluationid,
		farEditedbook.authors,
		farEditedbook.title,
		farEditedbook.publisher,
		farEditedbook.publicationstatuscode,
		farEditedbook.pages,
		farEditedbook.isbn,
		farEditedbook.publicationyear,
		farEditedbook.volumenumber,
		farEditedbook.edition,
		farEditedbook.publicationcity,
		farEditedbook.webaddress,
		farEditedbook.translated,
		farEditedbook.additionalinfo,
		farEditedbook.dtcreated,
		farEditedbook.dtupdated,
		farEditedbook.userlastmodified,
		farEditedbook.ispublic,
		farEditedbook.activityid,
		farEditedbook.load_error,
		farEditedbook.data_origin,
		farEditedbook.created_ew_dttm,
		farEditedbook.lastupd_dw_dttm,
		farEditedbook.batch_sid ]

	farEditedbookHash = hashThisList( farEditedbookList )

	tgtFarEditedbook = BioPsFarEditedbooks(
		source_hash = farEditedbookHash,
		editedbookid = farEditedbook.editedbookid,
		src_sys_id = farEditedbook.src_sys_id,
		evaluationid = farEditedbook.evaluationid,
		authors = farEditedbook.authors,
		title = farEditedbook.title,
		publisher = farEditedbook.publisher,
		publicationstatuscode = farEditedbook.publicationstatuscode,
		pages = farEditedbook.pages,
		isbn = farEditedbook.isbn,
		publicationyear = farEditedbook.publicationyear,
		volumenumber = farEditedbook.volumenumber,
		edition = farEditedbook.edition,
		publicationcity = farEditedbook.publicationcity,
		webaddress = farEditedbook.webaddress,
		translated = farEditedbook.translated,
		additionalinfo = farEditedbook.additionalinfo,
		dtcreated = farEditedbook.dtcreated,
		dtupdated = farEditedbook.dtupdated,
		userlastmodified = farEditedbook.userlastmodified,
		ispublic = farEditedbook.ispublic,
		activityid = farEditedbook.activityid,
		load_error = farEditedbook.load_error,
		data_origin = farEditedbook.data_origin,
		created_ew_dttm = farEditedbook.created_ew_dttm,
		lastupd_dw_dttm = farEditedbook.lastupd_dw_dttm,
		batch_sid = farEditedbook.batch_sid )

	return tgtFarEditedbook

