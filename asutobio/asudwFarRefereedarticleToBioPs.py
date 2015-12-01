from models.asudwpsmodels import AsuDwPsFarRefereedarticles, AsuDwPsFarEvaluations, AsuPsBioFilters
from models.biopsmodels import BioPsFarRefereedarticles

from sharedProcesses import hashThisList

# the data pull
def getSourceFarRefereedarticlesData( sesSource ):
	"""
		Selects the data from the data wharehouse for the FarRefereedarticles model.
		@returns: the record set
	"""
	srcFilters = AsuPsBioFilters( sesSource )

	srcEmplidsSubQry = srcFilters.getAllBiodesignEmplidList( True )

	farEvals = sesSource.query(
		AsuDwPsFarEvaluations.evaluationid ).join(
			srcEmplidsSubQry, AsuDwPsFarEvaluations.emplid == srcEmplidsSubQry.c.emplid ).subquery()

	return sesSource.query(
		AsuDwPsFarRefereedarticles ).join(
			farEvals, AsuDwPsFarRefereedarticles.evaluationid == farEvals.c.evaluationid ).filter(
				AsuDwPsFarRefereedarticles.ispublic != 'N' ).all()

# the data load
def processFarRefereedarticleData( farRefereedarticle ):
	"""
		Process an AsuDwPsFarRefereedarticles object and prepare it for insert into the target BioPsFarRefereedarticles table
		@return: the sa add object 
	"""
	
	farRefereedarticleList = [
		farRefereedarticle.refereedarticleid,
		farRefereedarticle.src_sys_id,
		farRefereedarticle.evaluationid,
		farRefereedarticle.authors,
		farRefereedarticle.title,
		farRefereedarticle.journalname,
		farRefereedarticle.publicationstatuscode,
		farRefereedarticle.publicationyear,
		farRefereedarticle.volumenumber,
		farRefereedarticle.pages,
		farRefereedarticle.webaddress,
		farRefereedarticle.translated,
		farRefereedarticle.abstract,
		farRefereedarticle.additionalinfo,
		farRefereedarticle.dtcreated,
		farRefereedarticle.dtupdated,
		farRefereedarticle.userlastmodified,
		farRefereedarticle.ispublic,
		farRefereedarticle.activityid,
		farRefereedarticle.load_error,
		farRefereedarticle.data_origin,
		farRefereedarticle.created_ew_dttm,
		farRefereedarticle.lastupd_dw_dttm,
		farRefereedarticle.batch_sid ]

	farRefereedarticleHash = hashThisList( farRefereedarticleList )

	tgtFarRefereedarticle = BioPsFarRefereedarticles(
		source_hash = farRefereedarticleHash,
		refereedarticleid = farRefereedarticle.refereedarticleid,
		src_sys_id = farRefereedarticle.src_sys_id,
		evaluationid = farRefereedarticle.evaluationid,
		authors = farRefereedarticle.authors,
		title = farRefereedarticle.title,
		journalname = farRefereedarticle.journalname,
		publicationstatuscode = farRefereedarticle.publicationstatuscode,
		publicationyear = farRefereedarticle.publicationyear,
		volumenumber = farRefereedarticle.volumenumber,
		pages = farRefereedarticle.pages,
		webaddress = farRefereedarticle.webaddress,
		translated = farRefereedarticle.translated,
		abstract = farRefereedarticle.abstract,
		additionalinfo = farRefereedarticle.additionalinfo,
		dtcreated = farRefereedarticle.dtcreated,
		dtupdated = farRefereedarticle.dtupdated,
		userlastmodified = farRefereedarticle.userlastmodified,
		ispublic = farRefereedarticle.ispublic,
		activityid = farRefereedarticle.activityid,
		load_error = farRefereedarticle.load_error,
		data_origin = farRefereedarticle.data_origin,
		created_ew_dttm = farRefereedarticle.created_ew_dttm,
		lastupd_dw_dttm = farRefereedarticle.lastupd_dw_dttm,
		batch_sid = farRefereedarticle.batch_sid )

	return tgtFarRefereedarticle


