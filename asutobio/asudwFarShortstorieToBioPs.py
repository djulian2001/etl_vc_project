from models.asudwpsmodels import AsuDwPsFarShortstories, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarShortstories

from sharedProcesses import hashThisList

# the data pull
def getSourceFarShortstoriesData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarShortstories model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarShortstories ).join(
			farEvals, AsuDwPsFarShortstories.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarShortstories.ispublic !='N' ).all()

# the data load
def processFarShortstorieData( farShortstories ):
	"""
		Process an AsuDwPsFarShortstories object and prepare it for insert into the target BioPsFarShortstories table
		@return: the sa add object 
	"""
	
	farShortstoriesList = [
		farShortstories.shortstoryid,
		farShortstories.src_sys_id,
		farShortstories.evaluationid,
		farShortstories.authors,
		farShortstories.title,
		farShortstories.publicationname,
		farShortstories.publicationstatuscode,
		farShortstories.pages,
		farShortstories.publicationyear,
		farShortstories.publisher,
		farShortstories.webaddress,
		farShortstories.translated,
		farShortstories.additionalinfo,
		farShortstories.dtcreated,
		farShortstories.dtupdated,
		farShortstories.userlastmodified,
		farShortstories.ispublic,
		farShortstories.activityid,
		farShortstories.load_error,
		farShortstories.data_origin,
		farShortstories.created_ew_dttm,
		farShortstories.lastupd_dw_dttm,
		farShortstories.batch_sid ]

	farShortstoriesHash = hashThisList( farShortstoriesList )

	tgtFarShortstorie = BioPsFarShortstories(
		source_hash = farShortstoriesHash,
		shortstoryid = farShortstories.shortstoryid,
		src_sys_id = farShortstories.src_sys_id,
		evaluationid = farShortstories.evaluationid,
		authors = farShortstories.authors,
		title = farShortstories.title,
		publicationname = farShortstories.publicationname,
		publicationstatuscode = farShortstories.publicationstatuscode,
		pages = farShortstories.pages,
		publicationyear = farShortstories.publicationyear,
		publisher = farShortstories.publisher,
		webaddress = farShortstories.webaddress,
		translated = farShortstories.translated,
		additionalinfo = farShortstories.additionalinfo,
		dtcreated = farShortstories.dtcreated,
		dtupdated = farShortstories.dtupdated,
		userlastmodified = farShortstories.userlastmodified,
		ispublic = farShortstories.ispublic,
		activityid = farShortstories.activityid,
		load_error = farShortstories.load_error,
		data_origin = farShortstories.data_origin,
		created_ew_dttm = farShortstories.created_ew_dttm,
		lastupd_dw_dttm = farShortstories.lastupd_dw_dttm,
		batch_sid = farShortstories.batch_sid )

	return tgtFarShortstorie
